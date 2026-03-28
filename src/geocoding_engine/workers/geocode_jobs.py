#sales_router/src/geocoding_engine/workers/geocode_jobs.py

import json
import sys
import logging
from rq import get_current_job
from uuid import uuid4
import os
import time
import gc

import pandas as pd
import redis

from rq import Queue
from geocoding_engine.visualization.geojson_builder import GeoJSONBuilder

logger = logging.getLogger("geocode_jobs")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# =========================================================
# HELPERS
# =========================================================

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def montar_addresses(df: pd.DataFrame):
    addresses = []

    for idx, row in df.iterrows():
        logradouro = row.get("logradouro")
        numero = row.get("numero")
        bairro = row.get("bairro")
        cidade = row.get("cidade")
        uf = row.get("uf")
        cep = row.get("cep")

        partes = []

        if pd.notna(logradouro):
            partes.append(str(logradouro).strip())

        if pd.notna(numero) and str(numero).strip():
            if partes:
                partes[-1] = f"{partes[-1]} {str(numero).strip()}"

        if pd.notna(bairro) and str(bairro).strip():
            partes.append(str(bairro).strip())

        if pd.notna(cidade) and pd.notna(uf):
            partes.append(f"{str(cidade).strip()} - {str(uf).strip()}")

        endereco = ", ".join(partes)

        addresses.append({
            "id": str(idx),
            "address": endereco,
            "cidade": None if pd.isna(cidade) else str(cidade).strip(),
            "uf": None if pd.isna(uf) else str(uf).strip(),
            "cep": None if pd.isna(cep) else str(cep).strip()
        })

    return addresses


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _get_hash_stats(redis_conn, stats_key: str):
    try:
        data = redis_conn.hgetall(stats_key) or {}
        return {
            "cache_hits": _safe_int(data.get("cache_hits", 0)),
            "nominatim_hits": _safe_int(data.get("nominatim_hits", 0)),
            "google_hits": _safe_int(data.get("google_hits", 0)),
            "falhas": _safe_int(data.get("falhas", 0)),
            "chunks_done": _safe_int(data.get("chunks_done", 0)),
            "chunks_failed": _safe_int(data.get("chunks_failed", 0)),
            "results_count": _safe_int(data.get("results_count", 0))
        }
    except Exception as e:
        logger.warning(f"[REDIS][HGETALL][ERRO] key={stats_key} erro={e}")
        return {
            "cache_hits": 0,
            "nominatim_hits": 0,
            "google_hits": 0,
            "falhas": 0,
            "chunks_done": 0,
            "chunks_failed": 0,
            "results_count": 0
        }


# =========================================================
# JOB PRINCIPAL
# =========================================================

def processar_geocode(file_path):
    job = get_current_job()
    job_id = str(job.id) if job and job.id else str(uuid4())
    started_at = time.time()

    OUTPUT_DIR = "/app/data/geocode_outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_file = f"{OUTPUT_DIR}/{job_id}.xlsx"
    output_json = f"{OUTPUT_DIR}/{job_id}.json"

    logger.info(f"🚀 Geocode job iniciado | job_id={job_id}")

    redis_conn = redis.Redis(host="redis", port=6379)
    redis_str = redis.Redis(host="redis", port=6379, decode_responses=True)
    subjob_queue = Queue("geocode_subjobs", connection=redis_conn)

    CHUNK_SIZE = 50
    redis_results_key = f"geocode_result:{job_id}"
    redis_done_key = f"geocode_done:{job_id}"
    redis_stats_key = f"geocode_stats:{job_id}"

    df_final = pd.DataFrame()
    df_validos = pd.DataFrame()
    df_invalidos = pd.DataFrame()

    tenant_id = 1
    origem = "job_distribuido"

    if job:
        tenant_id = int(job.meta.get("tenant_id", 1))
        origem = str(job.meta.get("origem", "job_distribuido"))

        job.meta.update({
            "output_file": output_file,
            "output_json": output_json,
            "progress": 0,
            "step": "Inicializando",
            "status": "started"
        })
        job.save_meta()

    try:
        # =====================================================
        # LEITURA
        # =====================================================
        if job:
            job.meta["progress"] = 5
            job.meta["step"] = "Lendo arquivo"
            job.save_meta()

        df = pd.read_excel(file_path)
        df = df.reset_index(drop=True)

        for col in ["setor", "consultor", "cnpj", "razao_social", "nome_fantasia"]:
            if col not in df.columns:
                df[col] = None

        logger.info(f"[DEBUG] linhas carregadas={len(df)}")
        logger.info(f"[DEBUG] colunas_entrada={df.columns.tolist()}")

        # =====================================================
        # PREPARAÇÃO
        # =====================================================
        if job:
            job.meta["progress"] = 10
            job.meta["step"] = "Preparando endereços"
            job.save_meta()

        addresses = montar_addresses(df)

        if not addresses:
            raise ValueError("Nenhum endereço encontrado")

        logger.info(f"[DEBUG] total_addresses={len(addresses)}")

        # Limpeza forte de chaves da execução
        redis_str.delete(redis_results_key)
        redis_str.delete(redis_done_key)
        redis_str.delete(redis_stats_key)

        # =====================================================
        # ENQUEUE
        # =====================================================
        if job:
            job.meta["progress"] = 15
            job.meta["step"] = "Enfileirando subjobs"
            job.save_meta()

        subjob_ids = []
        total_chunks = (len(addresses) + CHUNK_SIZE - 1) // CHUNK_SIZE

        logger.info(f"📦 {total_chunks} chunks estimados")

        chunk_id = 0

        for chunk in chunk_list(addresses, CHUNK_SIZE):
            payload = {
                "chunk_id": chunk_id,
                "parent_id": job_id,
                "addresses": chunk,
                "tenant_id": tenant_id,
                "origem": origem
            }

            try:
                subjob = subjob_queue.enqueue(
                    "geocoding_engine.workers.geocode_subjob.processar_subjob",
                    payload,
                    job_timeout=1800
                )
                subjob_ids.append(subjob.id)
            except Exception as e:
                logger.error(f"[ENQUEUE][ERRO] chunk_id={chunk_id} erro={e}")

            if chunk_id % 10 == 0:
                logger.info(f"[ENQUEUE] chunk {chunk_id + 1}/{total_chunks}")

            chunk_id += 1

        if len(subjob_ids) != total_chunks:
            logger.warning(
                f"[ENQUEUE][DIVERGENCIA] enviados={len(subjob_ids)} estimados={total_chunks}"
            )

        logger.info(f"🚀 {len(subjob_ids)} subjobs enviados")

        # =====================================================
        # AGUARDAR CONCLUSÃO REAL DOS CHUNKS
        # =====================================================
        while True:
            finished_chunks = redis_str.scard(redis_done_key)
            pushed_items = redis_str.llen(redis_results_key)
            stats_live = _get_hash_stats(redis_str, redis_stats_key)

            progress = 20 + int((finished_chunks / total_chunks) * 60) if total_chunks else 80
            progress = min(progress, 80)

            logger.info(
                f"[WAIT] finished_chunks={finished_chunks}/{total_chunks} "
                f"pushed_items={pushed_items} stats={stats_live}"
            )

            if job:
                prev = job.meta.get("progress", 0)
                job.meta["progress"] = max(prev, progress)
                job.meta["step"] = f"Processando ({finished_chunks}/{total_chunks})"
                job.meta["status"] = "started"
                job.save_meta()

            if finished_chunks >= total_chunks:
                break

            time.sleep(1)

        logger.info("[WAIT] todos os chunks sinalizaram conclusão")

        # =====================================================
        # CONSOLIDAR
        # =====================================================
        if job:
            job.meta["progress"] = 85
            job.meta["step"] = "Consolidando resultados"
            job.save_meta()

        raw = redis_str.lrange(redis_results_key, 0, -1)
        logger.info(f"[DEBUG] redis_items={len(raw)}")

        dfs = []

        for item in raw:
            try:
                data = json.loads(item)
                logger.info(f"[REDIS_RAW] {data}")
            except Exception as e:
                logger.warning(f"[REDIS][CORRUPTO] erro={e}")
                continue

            results = data.get("results", [])
            chunk_id = data.get("chunk_id")
            microbatch_index = data.get("microbatch_index", -1)

            if not isinstance(results, list):
                logger.warning(
                    f"[REDIS][RESULTS_INVALIDO] chunk_id={chunk_id} tipo={type(results)}"
                )
                results = []

            if results:
                try:
                    df_part = pd.DataFrame(results)
                    logger.info(f"[DF_PART] {df_part.to_dict(orient='records')}")

                    if "id" not in df_part.columns:
                        logger.warning(f"[DATAFRAME][SEM_ID] chunk_id={chunk_id}")
                        continue

                    df_part["id"] = df_part["id"].astype(str)
                    df_part["chunk_id"] = int(chunk_id) if chunk_id is not None else -1
                    df_part["microbatch_index"] = int(microbatch_index)

                    for col in ["lat", "lon", "source"]:
                        if col not in df_part.columns:
                            df_part[col] = None

                    dfs.append(df_part[[
                        "id", "lat", "lon", "source", "chunk_id", "microbatch_index"
                    ]])

                except Exception as e:
                    logger.warning(f"[DATAFRAME][ERRO] {e}")

            del results
            del data

        if not dfs:
            raise RuntimeError("Nenhum resultado retornado dos subjobs")

        df_result = pd.concat(dfs, ignore_index=True)

        del dfs
        del raw
        gc.collect()

        logger.info(f"[DEBUG] total_results_rows={len(df_result)}")
        logger.info(f"[DEBUG] df_result_columns={df_result.columns.tolist()}")

        # =====================================================
        # GARANTIAS E DEDUP
        # =====================================================
        for col in ["lat", "lon"]:
            df_result[col] = pd.to_numeric(df_result[col], errors="coerce")

        source_priority = {
            "cache": 0,
            "google": 1,
            "nominatim": 2,
            "nominatim_structured": 2,
            "nominatim_free": 2,
            "falha": 99,
            None: 999
        }

        df_result["source_priority"] = (
            df_result["source"]
            .map(source_priority)
            .fillna(50)
        )

        # Melhor resultado por id:
        # 1) melhor source_priority
        # 2) lat/lon válidos primeiro
        # 3) chunk e microbatch estáveis só para auditoria
        df_result["has_coords"] = (
            df_result["lat"].notnull() & df_result["lon"].notnull()
        ).astype(int)

        df_result = (
            df_result
            .sort_values(
                by=["id", "has_coords", "source_priority", "chunk_id", "microbatch_index"],
                ascending=[True, False, True, True, True]
            )
            .drop_duplicates(subset=["id"], keep="first")
            .copy()
        )

        logger.info(f"[DEBUG] df_result_dedup={len(df_result)}")
        logger.info(
            f"[DEBUG] df_result_dedup_sample={df_result.head(10).to_dict(orient='records')}"
        )

        # =====================================================
        # PREPARAR DF ORIGINAL
        # =====================================================
        df["id"] = df.index.astype(str)

        logger.info(f"[DEBUG] df_ids_sample={df['id'].head(10).tolist()}")
        logger.info(f"[DEBUG] df_result_ids_sample={df_result['id'].head(10).tolist()}")

        cols_conflito = ["lat", "lon", "source"]
        cols_existentes = [c for c in cols_conflito if c in df.columns]

        if cols_existentes:
            logger.warning(f"[MERGE][DROP_COLS] removendo colunas antigas: {cols_existentes}")
            df = df.drop(columns=cols_existentes, errors="ignore")

        # =====================================================
        # MERGE
        # =====================================================
        df_final = df.merge(
            df_result[["id", "lat", "lon", "source"]],
            on="id",
            how="left"
        )

        logger.info(f"[MERGE] total={len(df_final)}")
        logger.info(f"[MERGE] columns={df_final.columns.tolist()}")

        if "lat" not in df_final.columns or "lon" not in df_final.columns:
            logger.error("[MERGE][ERRO] colunas lat/lon ausentes após merge")
            raise Exception(f"Merge inválido. Colunas: {df_final.columns.tolist()}")

        df_final["lat"] = pd.to_numeric(df_final["lat"], errors="coerce")
        df_final["lon"] = pd.to_numeric(df_final["lon"], errors="coerce")

        lat_notnull = df_final["lat"].notnull().sum()
        lon_notnull = df_final["lon"].notnull().sum()

        logger.info(f"[MERGE] lat_notnull={lat_notnull}")
        logger.info(f"[MERGE] lon_notnull={lon_notnull}")

        try:
            logger.info(
                f"[MERGE_SAMPLE] {df_final[['id','lat','lon','source']].head(10).to_dict(orient='records')}"
            )
        except Exception as e:
            logger.warning(f"[MERGE_SAMPLE][ERRO] {e}")

        # =====================================================
        # PADRONIZAÇÃO
        # =====================================================
        if job:
            job.meta["progress"] = 90
            job.meta["step"] = "Salvando"
            job.save_meta()

        colunas_padrao = [
            "cnpj", "razao_social", "nome_fantasia",
            "logradouro", "numero", "bairro", "cidade", "uf", "cep",
            "consultor", "setor",
            "id", "lat", "lon", "source"
        ]

        for col in colunas_padrao:
            if col not in df_final.columns:
                df_final[col] = None

        df_final = df_final[colunas_padrao].copy()

        # =====================================================
        # SEPARAÇÃO
        # =====================================================
        df_validos = df_final.dropna(subset=["lat", "lon"]).copy()
        df_invalidos = df_final[df_final["lat"].isnull() | df_final["lon"].isnull()].copy()

        df_invalidos["motivo_invalidacao"] = "falha_geocode"

        logger.info(f"[FINAL] validos={len(df_validos)} invalidos={len(df_invalidos)}")

        from geocoding_engine.domain.municipio_polygon_validator import ponto_dentro_municipio

        if not df_validos.empty:

            logger.info("[POLIGONO] iniciando validação por município")

            df_validos["cidade"] = df_validos["cidade"].astype(str).str.upper().str.strip()
            df_validos["uf"] = df_validos["uf"].astype(str).str.upper().str.strip()

            total_antes = len(df_validos)  # 🔥 CORRETO

            valido_mask = []

            for _, row in df_validos.iterrows():

                try:
                    ok = ponto_dentro_municipio(
                        row.get("lat"),
                        row.get("lon"),
                        row.get("cidade"),
                        row.get("uf")
                    )
                    valido_mask.append(ok is True)

                except Exception as e:
                    logger.warning(f"[POLIGONO][ERRO] erro={e}")
                    valido_mask.append(False)

            df_validos["valido_municipio"] = valido_mask

            df_invalidos_municipio = df_validos[~df_validos["valido_municipio"]].copy()
            df_validos = df_validos[df_validos["valido_municipio"]].copy()

            df_invalidos_municipio["motivo_invalidacao"] = "fora_municipio"

            df_invalidos = pd.concat(
                [df_invalidos, df_invalidos_municipio],
                ignore_index=True
            )

            total_rejeitados = len(df_invalidos_municipio)
            taxa = (total_rejeitados / total_antes) if total_antes else 0

            logger.info(
                f"[POLIGONO] validos={len(df_validos)} "
                f"invalidos_municipio={len(df_invalidos_municipio)} "
                f"taxa_rejeicao={taxa:.2%}"
            )
        # =====================================================
        # EXCEL
        # =====================================================
        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            df_validos.to_excel(writer, sheet_name="geocodificados", index=False)
            df_invalidos.to_excel(writer, sheet_name="invalidos", index=False)

        logger.info("✅ Excel salvo")

        # =====================================================
        # JSON
        # =====================================================
        result = df_validos[["lat", "lon"]].copy()

        # 🔥 CRIAR ENDEREÇO
        result = df_validos[["lat", "lon"]].copy()

        result["endereco"] = (
            df_validos["logradouro"].fillna("").astype(str) + " " +
            df_validos["numero"].fillna("").astype(str) + ", " +
            df_validos["bairro"].fillna("").astype(str) + ", " +
            df_validos["cidade"].fillna("").astype(str) + " - " +
            df_validos["uf"].fillna("").astype(str)
        ).str.replace(" ,", ",").str.strip()

        if "cidade" in df_validos.columns:
            result["cidade"] = df_validos["cidade"]

        if "endereco" in df_validos.columns:
            result["endereco"] = df_validos["endereco"]

        if "setor" in df_validos.columns:
            result["setor"] = df_validos["setor"]

        # 🔥 ESSA LINHA FALTAVA
        if "consultor" in df_validos.columns:
            result["consultor"] = df_validos["consultor"]      

        # =====================================================
        # LIMPEZA (EVITA ERRO 500 JSON)
        # =====================================================

        import numpy as np

        result = result.replace([np.inf, -np.inf], None)
        result = result.where(pd.notnull(result), None)

        # =====================================================
        # CONVERSÃO SEGURA (SEM NaN / INF)
        # =====================================================

        import math
        import numpy as np

        def sanitize_all(v):

            if isinstance(v, (np.integer,)):
                return int(v)

            if isinstance(v, (np.floating,)):
                v = float(v)

            if isinstance(v, (int, float)):
                if math.isnan(v) or math.isinf(v):
                    return None

            if pd.isna(v):
                return None

            return str(v) if not isinstance(v, (int, float, type(None))) else v


        records = []

        for _, row in result.iterrows():

            logger.info(
                f"[FINAL_JSON_DEBUG] lat={row['lat']} lon={row['lon']} tipo_lat={type(row['lat'])}"
            )

            records.append({
                "lat": sanitize_all(row["lat"]),
                "lon": sanitize_all(row["lon"]),
                "cidade": sanitize_all(row.get("cidade")),
                "setor": sanitize_all(row.get("setor")),
                "endereco": sanitize_all(row.get("endereco")),
                "consultor": sanitize_all(row.get("consultor")),
            })
        # =====================================================
        # JSON (ARRAY PARA FRONTEND)
        # =====================================================

        # DEBUG
        logger.info(f"[JSON_SAMPLE] {records[:3]}")

        with open(output_json, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False)

        logger.info("✅ JSON (array) salvo")

        # =====================================================
        # GEOJSON (PARA MAPA AVANÇADO)
        # =====================================================

        geojson = GeoJSONBuilder.build(records)

        output_geojson = output_json.replace(".json", "_geo.json")

        with open(output_geojson, "w", encoding="utf-8") as f:
            json.dump(geojson, f, ensure_ascii=False)

        logger.info("✅ GEOJSON salvo")

        # =====================================================
        # HISTÓRICO
        # =====================================================
        live_stats = _get_hash_stats(redis_str, redis_stats_key)

        total = int(len(df_final))
        sucesso = int(len(df_validos))
        falhas = int(len(df_invalidos))
        tempo_ms = int((time.time() - started_at) * 1000)

        try:
            from geocoding_engine.infrastructure.geocoding_history_repository import GeocodingHistoryRepository
            from geocoding_engine.infrastructure.database_reader import DatabaseReader

            reader = DatabaseReader()
            conn = reader.conn
            repo = GeocodingHistoryRepository(conn)

            repo.salvar(
                request_id=job_id,
                tenant_id=tenant_id,
                origem=origem,
                total=total,
                sucesso=sucesso,
                falhas=falhas,
                cache_hits=int(live_stats["cache_hits"]),
                nominatim_hits=int(live_stats["nominatim_hits"]),
                google_hits=int(live_stats["google_hits"]),
                tempo_ms=tempo_ms
            )

            logger.info("✅ Histórico salvo")
        except Exception as e:
            logger.warning(f"[HISTORICO][ERRO] {e}")

        if job:
            job.meta.update({
                "progress": 100,
                "step": "Concluído",
                "status": "finished",
                "result": {
                    "total": total,
                    "sucesso": sucesso,
                    "falhas": falhas,
                    "cache_hits": int(live_stats["cache_hits"]),
                    "nominatim_hits": int(live_stats["nominatim_hits"]),
                    "google_hits": int(live_stats["google_hits"]),
                    "tempo_ms": tempo_ms
                }
            })
            job.save_meta()

        logger.info("✅ FINAL COMPLETO COM HISTÓRICO")

        return {
            "status": "finished",
            "total": total,
            "sucesso": sucesso,
            "falhas": falhas,
            "cache_hits": int(live_stats["cache_hits"]),
            "nominatim_hits": int(live_stats["nominatim_hits"]),
            "google_hits": int(live_stats["google_hits"]),
            "tempo_ms": tempo_ms
        }

    finally:
        try:
            redis_str.delete(redis_results_key)
            redis_str.delete(redis_done_key)
            redis_str.delete(redis_stats_key)
        except Exception as e:
            logger.warning(f"[REDIS][CLEANUP][ERRO] {e}")

        gc.collect()
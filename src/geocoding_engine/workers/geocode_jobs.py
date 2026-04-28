#sales_router/src/geocoding_engine/workers/geocode_jobs.py

import json
import sys
import logging
from rq import get_current_job
from uuid import uuid4
import os
import time
import gc
import re
import unicodedata

import pandas as pd
import redis

from rq import Queue
from geocoding_engine.domain.utils_texto import fix_encoding
from geocoding_engine.visualization.geojson_builder import GeoJSONBuilder
from geocoding_engine.application.reprocess_invalids_service import geocode_google_direto

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

def _normalize_text(txt: str) -> str:
    if not txt:
        return ""

    # remove acento
    txt = unicodedata.normalize("NFKD", str(txt))
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    txt = txt.replace("/", " ")
    txt = txt.replace(" - ", " ")
    txt = txt.replace("-", " ")
    txt = re.sub(r"[.,;:]+$", "", txt)

    # upper + trim
    txt = txt.upper().strip()

    # remove múltiplos espaços
    txt = re.sub(r"\s+", " ", txt)

    return txt


def _normalize_city_strict(cidade: str | None, uf: str | None = None) -> str | None:
    cidade = _normalize_text(cidade)
    if not cidade:
        return None

    uf_norm = _normalize_text(uf)
    if uf_norm and len(uf_norm) == 2:
        cidade = re.sub(rf"\b{re.escape(uf_norm)}\b$", "", cidade).strip()
        cidade = re.sub(rf"\b{re.escape(uf_norm)}\b", "", cidade).strip()
        cidade = re.sub(r"\s+", " ", cidade).strip()

    return cidade or None


def cidade_existe_ibge(cidade, uf, gdf_municipios):
    """
    Valida se cidade + UF existem no IBGE.

    Regras:
    - Normaliza acento
    - Case insensitive
    - Remove espaços duplicados
    - Fail fast se dado inválido
    """

    # -------------------------------------------------
    # 🔴 FAIL FAST
    # -------------------------------------------------
    if not cidade or not uf:
        return False

    cidade = _normalize_city_strict(cidade, uf=uf)
    uf = _normalize_text(uf)

    if not cidade or not uf or len(uf) != 2:
        return False

    # -------------------------------------------------
    # 🔥 GARANTE NORMALIZAÇÃO NO GDF (UMA VEZ SÓ)
    # -------------------------------------------------
    if "cidade_norm" not in gdf_municipios.columns:
        gdf_municipios["cidade_norm"] = [
            _normalize_city_strict(cidade_ref, uf=uf_ref)
            for cidade_ref, uf_ref in zip(gdf_municipios["cidade"], gdf_municipios["uf"])
        ]

    if "uf_norm" not in gdf_municipios.columns:
        gdf_municipios["uf_norm"] = gdf_municipios["uf"].apply(_normalize_text)

    # -------------------------------------------------
    # 🔍 MATCH
    # -------------------------------------------------
    match = gdf_municipios[
        (gdf_municipios["cidade_norm"] == cidade) &
        (gdf_municipios["uf_norm"] == uf)
    ]

    return not match.empty


def _build_ibge_city_lookup(gdf_municipios):
    if "cidade_norm" not in gdf_municipios.columns:
        gdf_municipios["cidade_norm"] = [
            _normalize_city_strict(cidade_ref, uf=uf_ref)
            for cidade_ref, uf_ref in zip(gdf_municipios["cidade"], gdf_municipios["uf"])
        ]

    if "uf_norm" not in gdf_municipios.columns:
        gdf_municipios["uf_norm"] = gdf_municipios["uf"].apply(_normalize_text)

    return set(
        zip(
            gdf_municipios["cidade_norm"].astype(str),
            gdf_municipios["uf_norm"].astype(str),
        )
    )


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _normalize_column_name(column_name: str) -> str:
    column_name = str(column_name or "").strip().lower()
    column_name = unicodedata.normalize("NFKD", column_name)
    column_name = "".join(ch for ch in column_name if not unicodedata.combining(ch))
    return column_name


def _extract_logradouro_numero(logradouro_raw: str):
    if not logradouro_raw:
        return "", ""

    logradouro_raw = str(logradouro_raw).strip()
    match = re.search(r"^(.*?)(?:\s*,?\s*)(\d{1,6})$", logradouro_raw)
    if match:
        return match.group(1).strip(), match.group(2)

    return logradouro_raw, ""


def _normalize_logradouro(logradouro_raw: str) -> str:
    if not logradouro_raw:
        return ""

    logradouro = fix_encoding(str(logradouro_raw).strip())
    logradouro = logradouro.split(",")[0].strip()
    return " ".join(logradouro.split())


def _normalize_numero(numero_raw: str) -> str:
    numero = fix_encoding(str(numero_raw or "").strip()).upper()
    if numero in ("", "0", "SN", "S/N", "SEM NUMERO", "SEM NÚMERO"):
        return ""
    return numero


def _clean_bairro(bairro: str, cidade: str) -> str:
    if not bairro:
        return ""

    bairro = fix_encoding(str(bairro).strip())
    cidade = fix_encoding(str(cidade).strip())

    if cidade and bairro.upper().endswith(" " + cidade.upper()):
        return bairro[:-(len(cidade) + 1)].strip()

    return bairro


def _normalize_operational_input(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [_normalize_column_name(col) for col in df.columns]

    text_columns = [
        "cnpj",
        "razao_social",
        "nome_fantasia",
        "logradouro",
        "numero",
        "bairro",
        "cidade",
        "uf",
        "cep",
        "consultor",
        "setor",
    ]

    for column in text_columns:
        if column in df.columns:
            df[column] = (
                df[column]
                .fillna("")
                .astype(str)
                .map(lambda value: fix_encoding(value.strip()))
                .replace({"nan": "", "None": ""})
            )

    if "cep" in df.columns:
        df["cep"] = df["cep"].str.replace(r"[^0-9]", "", regex=True)

    if "uf" in df.columns:
        df["uf"] = df["uf"].str.upper().str.strip()

    if "cidade" in df.columns:
        df["cidade"] = df["cidade"].map(lambda value: fix_encoding(str(value)).upper().strip())

    if "logradouro" in df.columns:
        if "numero" not in df.columns:
            df["numero"] = ""
        if "bairro" not in df.columns:
            df["bairro"] = ""
        if "cidade" not in df.columns:
            df["cidade"] = ""

        for idx, row in df.iterrows():
            logradouro_raw = row.get("logradouro", "")
            numero_raw = row.get("numero", "")
            logradouro_extraido, numero_extraido = _extract_logradouro_numero(logradouro_raw)
            numero_final = numero_raw if str(numero_raw).strip() else numero_extraido

            df.at[idx, "logradouro"] = _normalize_logradouro(logradouro_extraido)
            df.at[idx, "numero"] = _normalize_numero(numero_final)
            df.at[idx, "bairro"] = _clean_bairro(row.get("bairro", ""), row.get("cidade", ""))

    return df

def validar_input_basico(row):

    obrigatorios = ["logradouro", "numero", "cidade", "uf"]

    for campo in obrigatorios:
        if pd.isna(row.get(campo)) or not str(row.get(campo)).strip():
            return False, f"{campo}_invalido"

    return True, None

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
            partes[-1] = f"{partes[-1]} {str(numero).strip()}"

        if pd.notna(bairro) and str(bairro).strip():
            partes.append(str(bairro).strip())

        if pd.notna(cidade) and pd.notna(uf):
            partes.append(f"{str(cidade).strip()} - {str(uf).strip()}")

        endereco = ", ".join(partes)

        valido, motivo = validar_input_basico(row)

        if not valido:
            addresses.append({
                "id": str(idx),
                "address": None,
                "logradouro": None,
                "numero": None,
                "cidade": None,
                "uf": None,
                "cep": None,
                "erro": motivo
            })
            continue

        addresses.append({
            "id": str(idx),
            "address": endereco,
            "logradouro": str(logradouro).strip() if pd.notna(logradouro) else None,
            "numero": str(numero).strip() if pd.notna(numero) else None,
            "cidade": str(cidade).strip() if pd.notna(cidade) else None,
            "uf": str(uf).strip() if pd.notna(uf) else None,
            "cep": str(cep).strip() if pd.notna(cep) else None
        })

    return addresses


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return None


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


def _update_job_meta(job, progress: int, step: str, status: str = "started"):
    if not job:
        return

    job.meta["progress"] = progress
    job.meta["step"] = step
    job.meta["status"] = status
    job.save_meta()


def _append_missing_invalid_rows(df_base: pd.DataFrame, df_extra: pd.DataFrame) -> pd.DataFrame:
    if df_extra is None or df_extra.empty:
        return df_base

    if "id" not in df_base.columns or "id" not in df_extra.columns:
        return df_base

    ids_existentes = set(df_base["id"].astype(str))
    df_extra = df_extra.copy()
    df_extra["id"] = df_extra["id"].astype(str)
    df_extra = df_extra[~df_extra["id"].isin(ids_existentes)]

    if df_extra.empty:
        return df_base

    for col in df_base.columns:
        if col not in df_extra.columns:
            df_extra[col] = None

    df_extra = df_extra[df_base.columns.tolist()]

    return pd.concat([df_base, df_extra], ignore_index=True)


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

    CHUNK_SIZE = int(os.getenv("GEOCODE_BATCH_JSON_CHUNK_SIZE", "100"))
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
            "step": "Preparando seu processamento",
            "status": "started"
        })
        job.save_meta()

    try:
        # =====================================================
        # LEITURA
        # =====================================================
        if job:
            _update_job_meta(job, 5, "Lendo seu arquivo")

        df = pd.read_excel(file_path, dtype=str, engine="openpyxl")
        df = _normalize_operational_input(df).fillna("")
        df = df.reset_index(drop=True)

        # 🔥 DEFINE ID GLOBAL (CRÍTICO)
        df["id"] = df.index.astype(str)

        # 🔥 GUARDA ORIGINAL
        df_original = df.copy()

        # =====================================================
        # 🔥 VALIDAÇÃO DE CIDADE (ANTES DE TUDO)
        # =====================================================
        from geocoding_engine.domain.municipio_polygon_validator import carregar_municipios_gdf

        if job:
            _update_job_meta(job, 8, "Conferindo seus dados")

        gdf_municipios = carregar_municipios_gdf()

        if job:
            _update_job_meta(job, 12, "Validando informacoes do arquivo")

        df["cidade"] = df["cidade"].astype(str).str.upper().str.strip()
        df["uf"] = df["uf"].astype(str).str.upper().str.strip()

        ibge_city_lookup = _build_ibge_city_lookup(gdf_municipios)
        df["cidade_norm"] = [
            _normalize_city_strict(cidade, uf=uf)
            for cidade, uf in zip(df["cidade"], df["uf"])
        ]
        df["uf_norm"] = df["uf"].apply(_normalize_text)

        df["cidade_valida"] = [
            (cidade_norm, uf_norm) in ibge_city_lookup
            if cidade_norm and uf_norm and len(uf_norm) == 2
            else False
            for cidade_norm, uf_norm in zip(df["cidade_norm"], df["uf_norm"])
        ]

        df_invalidos_cidade = df[~df["cidade_valida"]].copy()

        if not df_invalidos_cidade.empty:
            logger.warning(f"[CIDADE_INVALIDA] total={len(df_invalidos_cidade)}")

        # 🔥 SEGUE SÓ COM VÁLIDOS
        df_invalidos_cidade["motivo_invalidacao"] = "cidade_invalida"
        df_invalidos_cidade["status_final"] = "invalido"

        # 🔥 mantém separado
        df_validos_input = df[df["cidade_valida"]].copy()

        df = df_validos_input.drop(columns=["cidade_norm", "uf_norm"], errors="ignore").copy()

        for col in ["setor", "consultor", "cnpj", "razao_social", "nome_fantasia"]:
            if col not in df.columns:
                df[col] = None

        logger.info(f"[DEBUG] linhas carregadas={len(df)}")
        logger.info(f"[DEBUG] colunas_entrada={df.columns.tolist()}")

        # =====================================================
        # PREPARAÇÃO
        # =====================================================
        addresses = montar_addresses(df)

        if not addresses:
            raise ValueError("Nenhum endereço encontrado")

        logger.info(f"[DEBUG] total_addresses={len(addresses)}")

        # 🔥 NOVO — separação
        addresses_validos = [a for a in addresses if a.get("address")]
        addresses_invalidos_input = [a for a in addresses if not a.get("address")]

        logger.info(f"[INPUT] validos={len(addresses_validos)} invalidos={len(addresses_invalidos_input)}")

        # 🔥 NOVO — dataframe de inválidos de entrada
        df_invalidos_input = pd.DataFrame(addresses_invalidos_input)

        if not df_invalidos_input.empty:
            df_invalidos_input["motivo_invalidacao"] = df_invalidos_input["erro"]

        # Limpeza Redis
        redis_str.delete(redis_results_key)
        redis_str.delete(redis_done_key)
        redis_str.delete(redis_stats_key)

        # =====================================================
        # ENQUEUE
        # =====================================================
        if job:
            _update_job_meta(job, 15, "Organizando as etapas do processamento")

        subjob_ids = []
        total_chunks = (len(addresses_validos) + CHUNK_SIZE - 1) // CHUNK_SIZE

        logger.info(f"📦 {total_chunks} chunks estimados")

        chunk_id = 0

        for chunk in chunk_list(addresses_validos, CHUNK_SIZE):

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

            chunk_id += 1

        # =====================================================
        # AGUARDAR CONCLUSÃO REAL DOS CHUNKS
        # =====================================================
        while True:
            finished_chunks = redis_str.scard(redis_done_key)
            pushed_items = redis_str.llen(redis_results_key)
            stats_live = _get_hash_stats(redis_str, redis_stats_key)

            progress = 20 + int((finished_chunks / total_chunks) * 60) if total_chunks else 80
            progress = min(progress, 80)

            logger.debug(
                f"[WAIT] finished_chunks={finished_chunks}/{total_chunks} "
                f"pushed_items={pushed_items} stats={stats_live}"
            )

            if job:
                prev = job.meta.get("progress", 0)
                job.meta["progress"] = max(prev, progress)
                job.meta["step"] = f"Localizando enderecos ({finished_chunks}/{total_chunks})"
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
            _update_job_meta(job, 85, "Organizando os resultados")

        raw = redis_str.lrange(redis_results_key, 0, -1)
        logger.info(f"[DEBUG] redis_items={len(raw)}")

        dfs = []

        for item in raw:
            try:
                data = json.loads(item)
                logger.debug(f"[REDIS_RAW] {data}")
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
                    logger.debug(f"[DF_PART] {df_part.to_dict(orient='records')}")

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

        logger.debug(f"[DEBUG] total_results_rows={len(df_result)}")
        logger.debug(f"[DEBUG] df_result_columns={df_result.columns.tolist()}")

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

        logger.debug(f"[DEBUG] df_result_dedup={len(df_result)}")
        logger.debug(
            f"[DEBUG] df_result_dedup_sample={df_result.head(10).to_dict(orient='records')}"
        )

        # =====================================================
        # PREPARAR DF ORIGINAL
        # =====================================================

        logger.debug(f"[DEBUG] df_ids_sample={df['id'].head(10).tolist()}")
        logger.debug(f"[DEBUG] df_result_ids_sample={df_result['id'].head(10).tolist()}")

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
            logger.debug(
                f"[MERGE_SAMPLE] {df_final[['id','lat','lon','source']].head(10).to_dict(orient='records')}"
            )
        except Exception as e:
            logger.debug(f"[MERGE_SAMPLE][ERRO] {e}")

        # =====================================================
        # PADRONIZAÇÃO
        # =====================================================
        if job:
            _update_job_meta(job, 90, "Conferindo a qualidade final")

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
        df_invalidos_sem_coords = df_final[
            df_final["lat"].isna() &
            df_final["lon"].isna() &
            df_final["source"].notna()
        ].copy()
        if not df_invalidos_sem_coords.empty:
            df_invalidos_sem_coords["motivo_invalidacao"] = (
                df_invalidos_sem_coords["source"].fillna("falha_geocode")
            )

        logger.info(f"[FINAL] validos={len(df_validos)} invalidos={len(df_invalidos)}")
        df_invalidos_criticos = pd.DataFrame()

        # =====================================================
        # VALIDAÇÃO DE MUNICÍPIO (OTIMIZADA)
        # =====================================================
        from geocoding_engine.domain.geo_validator import validar_municipios_batch_fast
        
        df_invalidos_criticos = pd.DataFrame()

        if not df_validos.empty:
            if job:
                _update_job_meta(job, 92, "Validando a consistencia dos resultados")

            logger.info("[POLIGONO] validação batch iniciada")

            df_validos["cidade"] = df_validos["cidade"].astype(str).str.upper().str.strip()
            df_validos["uf"] = df_validos["uf"].astype(str).str.upper().str.strip()

            df_validos = validar_municipios_batch_fast(df_validos, gdf_municipios)

            # 🔥 separação correta
            df_invalidos_criticos = df_validos[~df_validos["valido_municipio"]].copy()
            df_invalidos_criticos["motivo_invalidacao"] = "fora_municipio"

            df_validos = df_validos[df_validos["valido_municipio"]].copy()

            # 🔥 FAIL FAST (CRÍTICO)
            if "id" not in df_validos.columns:
                raise Exception("[ERRO CRÍTICO] df_validos perdeu coluna 'id'")

            if "id" not in df_invalidos_criticos.columns:
                raise Exception("[ERRO CRÍTICO] df_invalidos_criticos perdeu coluna 'id'")

            # 🔥 GARANTE TIPO
            df_validos["id"] = df_validos["id"].astype(str)
            df_invalidos_criticos["id"] = df_invalidos_criticos["id"].astype(str)

            logger.info(
                f"[POLIGONO] validos={len(df_validos)} invalidos_municipio={len(df_invalidos_criticos)}"
            )

        # =====================================================
        # 🔥 REPROCESSAMENTO CONTROLADO
        # =====================================================
        from geocoding_engine.application.reprocess_invalids_service import ReprocessInvalidsService
        from geocoding_engine.infrastructure.database_writer import DatabaseWriter
        from geocoding_engine.infrastructure.database_reader import DatabaseReader

        if job:
            _update_job_meta(job, 94, "Fazendo uma nova conferencia dos casos pendentes")

        logger.info("♻ Reprocessamento iniciado")

        df_reprocessar = pd.concat(
            [df_invalidos_criticos, df_invalidos_sem_coords],
            ignore_index=True,
        )
        if not df_reprocessar.empty and "id" in df_reprocessar.columns:
            df_reprocessar["id"] = df_reprocessar["id"].astype(str)
            df_reprocessar = df_reprocessar.drop_duplicates(subset=["id"], keep="first")

        logger.info(f"♻ Reprocessando: {len(df_reprocessar)}")

        df_recuperados = pd.DataFrame()
        df_restantes = df_reprocessar.copy()

        if not df_reprocessar.empty:

            reader = DatabaseReader()
            conn = reader.conn
            writer = DatabaseWriter(conn)

            reprocessor = ReprocessInvalidsService(writer)

            df_recuperados, df_restantes = reprocessor.execute(df_reprocessar)

        # =====================================================
        # 🔥 REVALIDAÇÃO DOS RECUPERADOS (CORRIGIDA)
        # =====================================================
        df_recuperados_validos = pd.DataFrame()
        df_recuperados_invalidos = pd.DataFrame()

        if not df_recuperados.empty:

            if "id" not in df_recuperados.columns:
                raise Exception("[ERRO CRÍTICO] df_recuperados sem coluna 'id'")

            df_recuperados["id"] = df_recuperados["id"].astype(str)

            # -------------------------------------------------
            # 🔥 SEPARA FALLBACK (CRÍTICO)
            # -------------------------------------------------
            df_fallback = df_recuperados[
                df_recuperados["source"] == "fallback_cidade"
            ].copy()

            df_nao_fallback = df_recuperados[
                df_recuperados["source"] != "fallback_cidade"
            ].copy()

            # -------------------------------------------------
            # 🔥 VALIDA SOMENTE NÃO-FALLBACK
            # -------------------------------------------------
            if not df_nao_fallback.empty:

                df_nao_fallback = validar_municipios_batch_fast(df_nao_fallback, gdf_municipios)

                df_nao_fallback_validos = df_nao_fallback[
                    df_nao_fallback["valido_municipio"]
                ].copy()

                df_nao_fallback_invalidos = df_nao_fallback[
                    ~df_nao_fallback["valido_municipio"]
                ].copy()

            else:
                df_nao_fallback_validos = pd.DataFrame()
                df_nao_fallback_invalidos = pd.DataFrame()


            # -------------------------------------------------
            # 🔥 NOVA REGRA — GOOGLE FORA DO POLÍGONO → FALLBACK
            # -------------------------------------------------
            if not df_nao_fallback_invalidos.empty:

                logger.warning("[GOOGLE_FORA_POLIGONO] → fallback cidade")

                df_fallback_extra = df_nao_fallback_invalidos.copy()

                for idx, row in df_fallback_extra.iterrows():

                    lat_fb, lon_fb = geocode_google_direto(f"{row['cidade']}, {row['uf']}, Brasil")

                    if lat_fb is not None and lon_fb is not None:
                        df_fallback_extra.at[idx, "lat"] = lat_fb
                        df_fallback_extra.at[idx, "lon"] = lon_fb
                        df_fallback_extra.at[idx, "source"] = "fallback_cidade"

            else:
                df_fallback_extra = pd.DataFrame()

            if not df_fallback_extra.empty:
                df_fallback_extra = validar_municipios_batch_fast(df_fallback_extra, gdf_municipios)

                df_fallback_extra_validos = df_fallback_extra[
                    df_fallback_extra["valido_municipio"]
                ].copy()

                df_fallback_extra_invalidos = df_fallback_extra[
                    ~df_fallback_extra["valido_municipio"]
                ].copy()
            else:
                df_fallback_extra_validos = pd.DataFrame()
                df_fallback_extra_invalidos = pd.DataFrame()


            # -------------------------------------------------
            # 🔥 FALLBACK ORIGINAL (mantém)
            # -------------------------------------------------
            if not df_fallback.empty:

                df_fallback = validar_municipios_batch_fast(df_fallback, gdf_municipios)

                fora = (~df_fallback["valido_municipio"]).sum()

                if fora > 0:
                    logger.warning(f"[FALLBACK_FORA_POLIGONO] total={fora}")

                df_fallback_validos = df_fallback[
                    df_fallback["valido_municipio"]
                ].copy()

                df_fallback_invalidos = df_fallback[
                    ~df_fallback["valido_municipio"]
                ].copy()

            else:
                df_fallback_validos = pd.DataFrame()
                df_fallback_invalidos = pd.DataFrame()


            # -------------------------------------------------
            # 🔥 CONSOLIDA FINAL
            # -------------------------------------------------
            df_recuperados_validos = pd.concat([
                df_nao_fallback_validos,
                df_fallback_validos,
                df_fallback_extra_validos
            ], ignore_index=True)


            df_recuperados_invalidos = pd.concat([
                df_nao_fallback_invalidos,
                df_fallback_invalidos,
                df_fallback_extra_invalidos
            ], ignore_index=True)

            logger.info(
                f"[REPROCESS_POLIGONO] validos={len(df_recuperados_validos)} "
                f"invalidos={len(df_recuperados_invalidos)} "
                f"(fallback_incluidos={len(df_fallback_validos) + len(df_fallback_extra_validos)})"
            )

        # =====================================================
        # 🔥 FALLBACK CONTROLADO (SEM CONTAMINAR)
        # =====================================================

        # 🔥 NÃO DESCARTA FALLBACK
        if not df_recuperados_invalidos.empty:
            logger.warning(
                f"[INVALIDOS_REAIS] fora do município (não fallback): {len(df_recuperados_invalidos)}"
            )

        # =====================================================
        # 🔥 CONSOLIDAÇÃO FINAL
        # =====================================================

        df_validos_final = pd.concat([
            df_validos,
            df_recuperados_validos
        ], ignore_index=True)

        df_validos_final = df_validos_final.drop_duplicates(subset=["id"], keep="first")

        # 🔥 FAIL FAST FINAL
        if "id" not in df_final.columns:
            raise Exception("[ERRO CRÍTICO] df_final sem coluna 'id'")

        logger.info(f"[FINAL] total={len(df_final)}")

        
        # =====================================================
        # 🔥 INVALIDOS REAIS (OPCIONAL EXPORT)
        # =====================================================

        dfs_invalidos = []

        if not df_invalidos_criticos.empty:
            dfs_invalidos.append(df_invalidos_criticos)

        if not df_recuperados_invalidos.empty:
            dfs_invalidos.append(df_recuperados_invalidos)

        df_invalidos_final = pd.concat(dfs_invalidos, ignore_index=True) if dfs_invalidos else pd.DataFrame()

        logger.info(f"[INVALIDOS_FINAL] total={len(df_invalidos_final)}")

        # =====================================================
        # 🔥 CACHE SEGURO
        # =====================================================

        df_cache = df_validos_final.copy()

        if "valido_municipio" in df_cache.columns:
            df_cache = df_cache[df_cache["valido_municipio"] == True]

        
        # =====================================================
        # 🔥 PADRONIZAÇÃO INVALIDOS INPUT (CRÍTICO)
        # =====================================================

        if not df_invalidos_input.empty:
            for col in df_final.columns:
                if col not in df_invalidos_input.columns:
                    df_invalidos_input[col] = None
        
        # ======================================================
        # CONSOLIDAÇÃO FINAL CORRETA (SEM DUPLICAÇÃO)
        # ======================================================

        # 🔥 válidos finais já estão corretos
        df_validos = df_validos_final.copy()

        # 🔥 base correta = ORIGINAL (CRÍTICO)
        df_base = df_original.copy()

        # garante tipo
        df_base["id"] = df_base.index.astype(str)
        df_validos["id"] = df_validos["id"].astype(str)

        # 🔥 todos os IDs válidos
        ids_validos = set(df_validos["id"])

        # 🔥 inválidos = tudo que NÃO está nos válidos
        df_invalidos = df_base[~df_base["id"].isin(ids_validos)].copy()

        # -----------------------------------------------------
        # MOTIVO DE INVALIDAÇÃO
        # -----------------------------------------------------

        # default
        df_invalidos["motivo_invalidacao"] = "falha_geocode"

        # prioridade: polígono
        if not df_invalidos_criticos.empty:

            df_invalidos_criticos["id"] = df_invalidos_criticos["id"].astype(str)

            df_invalidos = df_invalidos.merge(
                df_invalidos_criticos[["id", "motivo_invalidacao"]],
                on="id",
                how="left",
                suffixes=("", "_critico")
            )

            df_invalidos["motivo_invalidacao"] = df_invalidos["motivo_invalidacao_critico"].combine_first(
                df_invalidos["motivo_invalidacao"]
            )

            df_invalidos.drop(columns=["motivo_invalidacao_critico"], inplace=True)

        # -----------------------------------------------------
        # INPUT INVALIDO (SEM DUPLICAR)
        # -----------------------------------------------------

        if not df_invalidos_input.empty:

            df_invalidos_input["id"] = df_invalidos_input["id"].astype(str)
            df_invalidos = _append_missing_invalid_rows(df_invalidos, df_invalidos_input)

        # -----------------------------------------------------
        # CIDADE INVALIDA (CRÍTICO)
        # -----------------------------------------------------

        if not df_invalidos_cidade.empty:

            df_invalidos_cidade = df_invalidos_cidade.copy()
            df_invalidos_cidade["id"] = df_invalidos_cidade["id"].astype(str)

            # padroniza colunas
            for col in df_invalidos.columns:
                if col not in df_invalidos_cidade.columns:
                    df_invalidos_cidade[col] = None

            df_invalidos = _append_missing_invalid_rows(df_invalidos, df_invalidos_cidade)

        # -----------------------------------------------------
        # DEDUP FINAL
        # -----------------------------------------------------

        df_invalidos = df_invalidos.drop_duplicates(subset=["id"], keep="first")

        # -----------------------------------------------------
        # STATUS FINAL
        # -----------------------------------------------------

        df_validos["status_final"] = "valido"
        df_invalidos["status_final"] = "invalido"

        # -----------------------------------------------------
        # CONSISTÊNCIA
        # -----------------------------------------------------

        total_final = len(df_validos) + len(df_invalidos)

        if total_final != len(df_original):
            logger.error(f"[ERRO_CONSISTENCIA] total_final={total_final} original={len(df_original)}")
        else:
            logger.info(f"[CONSISTENTE] total={len(df_original)} validos={len(df_validos)} invalidos={len(df_invalidos)}")
                                
        
        # =====================================================
        # 🔥 PERSISTÊNCIA DE CACHE FINAL (VERSÃO DEFINITIVA)
        # =====================================================

        from geocoding_engine.infrastructure.database_writer import DatabaseWriter
        from geocoding_engine.infrastructure.database_reader import DatabaseReader

        import math

        try:
            reader = DatabaseReader()
            conn = reader.conn
            writer = DatabaseWriter(conn)

            df_cache = df_validos_final.copy()

            # 🔥 GARANTE VALIDAÇÃO FINAL
            if "valido_municipio" in df_cache.columns:
                df_cache = df_cache[df_cache["valido_municipio"] == True]

            # 🔥 SOMENTE COM COORDENADA
            df_cache = df_cache[
                df_cache["lat"].notnull() &
                df_cache["lon"].notnull()
            ].copy()

            # 🔥 NORMALIZA CAMPOS
            df_cache["logradouro"] = df_cache["logradouro"].astype(str).str.strip()
            df_cache["numero"] = df_cache["numero"].astype(str).str.replace(".0", "").str.strip()
            df_cache["cidade"] = df_cache["cidade"].astype(str).str.strip()
            df_cache["uf"] = df_cache["uf"].astype(str).str.strip()

            # 🔥 DEDUP REAL
            df_cache = df_cache.drop_duplicates(
                subset=["logradouro", "numero", "cidade", "uf"]
            )

            logger.info(f"[CACHE_FINAL] candidatos={len(df_cache)}")

            saved = 0

            for _, row in df_cache.iterrows():

                # -------------------------------------------------
                # 🔴 NÃO SALVAR FALLBACK OU INVÁLIDO
                # -------------------------------------------------
                if (
                    str(row.get("source")) == "fallback_cidade"
                    or not row.get("valido_municipio", True)
                ):
                    continue

                logradouro = row.get("logradouro")
                numero = row.get("numero")
                cidade = row.get("cidade")
                uf = row.get("uf")

                if not cidade or not uf:
                    continue

                lat = row.get("lat")
                lon = row.get("lon")

                if lat is None or lon is None:
                    continue

                try:
                    if math.isnan(lat) or math.isnan(lon):
                        continue
                except Exception:
                    continue

                endereco_raw = (
                    f"{logradouro} {numero}, {cidade} - {uf}"
                ).replace(" ,", ",").strip()

                if len(endereco_raw) < 10:
                    continue

                if "NAN" in endereco_raw.upper():
                    continue

                try:
                    writer.salvar_cache(
                        logradouro=logradouro,
                        numero=numero,
                        cidade=cidade,
                        uf=uf,
                        endereco_original=endereco_raw,
                        lat=float(lat),
                        lon=float(lon),
                        origem=row.get("source")
                    )
                    saved += 1

                except Exception as e:
                    logger.warning(f"[CACHE_FINAL][ERRO] {e}")

            logger.info(f"[CACHE_FINAL] salvos={saved}")

        finally:
            try:
                if conn:
                    conn.close()
            except:
                pass

        # =====================================================
        # EXCEL
        # =====================================================
        if job:
            _update_job_meta(job, 97, "Preparando sua entrega")

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            df_validos_final.to_excel(writer, sheet_name="geocodificados", index=False)
            df_invalidos.to_excel(writer, sheet_name="invalidos", index=False)

        logger.info("✅ Excel salvo")

        
        # =====================================================
        # JSON
        # =====================================================
        result = df_validos_final[["lat", "lon"]].copy()

        
        # =====================================================
        # 🔥 CRIAR ENDEREÇO (CORRETO)
        # =====================================================

        result = df_validos_final[["lat", "lon"]].copy()

        result["endereco"] = (
            df_validos_final["logradouro"].fillna("").astype(str) + " " +
            df_validos_final["numero"].fillna("").astype(str) + ", " +
            df_validos_final["bairro"].fillna("").astype(str) + ", " +
            df_validos_final["cidade"].fillna("").astype(str) + " - " +
            df_validos_final["uf"].fillna("").astype(str)
        ).str.replace(" ,", ",").str.strip()

        # =====================================================
        # 🔥 CAMPOS ADICIONAIS
        # =====================================================

        if "cidade" in df_validos_final.columns:
            result["cidade"] = df_validos_final["cidade"]

        if "setor" in df_validos_final.columns:
            result["setor"] = df_validos_final["setor"]

        if "consultor" in df_validos_final.columns:
            result["consultor"] = df_validos_final["consultor"]

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

        logger.debug(f"[JSON_SAMPLE] {records[:3]}")

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

        total = int(len(df_original))
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
                "step": "Processamento concluido",
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

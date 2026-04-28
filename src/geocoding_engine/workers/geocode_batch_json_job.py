import json
import logging
import os
import re
import sys
import time
import threading
import unicodedata
from uuid import uuid4

import pandas as pd
import redis
from rq import Queue, Retry, get_current_job
from rq.job import Job

from geocoding_engine.application.reprocess_invalids_service import (
    ReprocessInvalidsService,
    geocode_google_direto,
)
from geocoding_engine.domain.geo_validator import validar_municipios_batch_fast
from geocoding_engine.domain.municipio_polygon_validator import (
    _norm_uf,
    carregar_municipios_gdf,
)
from geocoding_engine.infrastructure.database_reader import DatabaseReader
from geocoding_engine.infrastructure.database_writer import DatabaseWriter


logger = logging.getLogger("geocode_batch_json_job")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


redis_conn = redis.Redis(host="redis", port=6379)
redis_str = redis.Redis(host="redis", port=6379, decode_responses=True)


def _set_job_progress(job, progress: int, step: str, status: str = "started", **extra):
    if not job:
        return

    current_progress = int(job.meta.get("progress", 0) or 0)
    job.meta.update({
        "progress": max(current_progress, progress),
        "step": step,
        "status": status,
        **extra,
    })
    job.save_meta()


def _start_progress_heartbeat(
    job,
    start: int,
    end: int,
    step: str,
    interval: float = 3.0,
):
    if not job or start >= end:
        return lambda: None

    stop_event = threading.Event()

    def run():
        progress = max(int(job.meta.get("progress", 0) or 0), start)

        while not stop_event.wait(interval):
            if progress >= end - 1:
                continue

            progress += 1
            _set_job_progress(job, progress, step)

    thread = threading.Thread(target=run, daemon=True)
    thread.start()

    def stop():
        stop_event.set()
        thread.join(timeout=0.1)

    return stop


def _chunk_list(items, size):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def _normalize_text(txt: str) -> str:
    if not txt:
        return ""

    txt = unicodedata.normalize("NFKD", str(txt))
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    txt = txt.replace("/", " ")
    txt = txt.replace(" - ", " ")
    txt = txt.replace("-", " ")
    txt = re.sub(r"[.,;:]+$", "", txt)
    txt = txt.upper().strip()
    txt = re.sub(r"\s+", " ", txt)
    return txt


def _normalize_city_strict(cidade: str | None, uf: str | None = None) -> str | None:
    cidade = _normalize_text(cidade)
    if not cidade:
        return None

    uf_norm = _norm_uf(uf)
    if uf_norm:
        cidade = re.sub(rf"\b{re.escape(uf_norm)}\b$", "", cidade).strip()
        cidade = re.sub(rf"\b{re.escape(uf_norm)}\b", "", cidade).strip()
        cidade = re.sub(r"\s+", " ", cidade).strip()

    return cidade or None


def _cidade_existe_ibge(cidade, uf, gdf_municipios):
    if not cidade or not uf:
        return False

    cidade = _normalize_city_strict(cidade, uf=uf)
    uf = _norm_uf(uf)

    if not cidade or not uf or len(uf) != 2:
        return False

    if "cidade_norm" not in gdf_municipios.columns:
        gdf_municipios["cidade_norm"] = [
            _normalize_city_strict(cidade_ref, uf=uf_ref)
            for cidade_ref, uf_ref in zip(gdf_municipios["cidade"], gdf_municipios["uf"])
        ]

    if "uf_norm" not in gdf_municipios.columns:
        gdf_municipios["uf_norm"] = [_norm_uf(uf_ref) for uf_ref in gdf_municipios["uf"]]

    match = gdf_municipios[
        (gdf_municipios["cidade_norm"] == cidade)
        & (gdf_municipios["uf_norm"] == uf)
    ]

    return not match.empty


def _build_ibge_city_lookup(gdf_municipios):
    if "cidade_norm" not in gdf_municipios.columns:
        gdf_municipios["cidade_norm"] = [
            _normalize_city_strict(cidade_ref, uf=uf_ref)
            for cidade_ref, uf_ref in zip(gdf_municipios["cidade"], gdf_municipios["uf"])
        ]

    if "uf_norm" not in gdf_municipios.columns:
        gdf_municipios["uf_norm"] = [_norm_uf(uf_ref) for uf_ref in gdf_municipios["uf"]]

    return set(
        zip(
            gdf_municipios["cidade_norm"].astype(str),
            gdf_municipios["uf_norm"].astype(str),
        )
    )


def _separar_cidades_invalidas(addresses, gdf_municipios):
    if not addresses:
        return [], pd.DataFrame()

    ibge_city_lookup = _build_ibge_city_lookup(gdf_municipios)
    validos = []
    invalidos = []

    for item in addresses:
        cidade_norm = _normalize_city_strict(item.get("cidade"), uf=item.get("uf"))
        uf_norm = _norm_uf(item.get("uf"))

        if cidade_norm and uf_norm and (cidade_norm, uf_norm) in ibge_city_lookup:
            validos.append(item)
            continue

        invalidos.append({
            **item,
            "motivo_invalidacao": "cidade_invalida",
        })

    return validos, pd.DataFrame(invalidos)


def _safe_int(v, default=0):
    try:
        return int(v)
    except Exception:
        return default


def _get_hash_stats(stats_key: str):
    data = redis_str.hgetall(stats_key) or {}
    return {
        "cache_hits": _safe_int(data.get("cache_hits")),
        "nominatim_hits": _safe_int(data.get("nominatim_hits")),
        "google_hits": _safe_int(data.get("google_hits")),
        "falhas": _safe_int(data.get("falhas")),
        "chunks_done": _safe_int(data.get("chunks_done")),
        "chunks_failed": _safe_int(data.get("chunks_failed")),
        "results_count": _safe_int(data.get("results_count")),
    }


def _mark_chunk_failed(
    results_key: str,
    done_key: str,
    stats_key: str,
    chunk_id: int,
    total: int,
    error: str,
):
    payload = {
        "chunk_id": chunk_id,
        "microbatch_index": -1,
        "results": [],
        "stats": {
            "total": total,
            "cache_hits": 0,
            "nominatim_hits": 0,
            "google_hits": 0,
            "falhas": total,
            "tempo_ms": 0,
        },
        "error": error,
    }

    redis_str.rpush(results_key, json.dumps(payload, ensure_ascii=False))
    redis_str.hincrby(stats_key, "falhas", total)
    redis_str.hincrby(stats_key, "chunks_failed", 1)
    redis_str.sadd(done_key, str(chunk_id))


def _coerce_result_rows(raw_items):
    rows = []

    for item in raw_items:
        try:
            payload = json.loads(item)
        except Exception as e:
            logger.warning(f"[BATCH_JSON][REDIS_CORRUPTO] {e}")
            continue

        for result in payload.get("results") or []:
            if not isinstance(result, dict):
                continue

            rows.append({
                "id": str(result.get("id")),
                "lat": result.get("lat"),
                "lon": result.get("lon"),
                "source": result.get("source") or "falha",
            })

    return rows


def _dedup_results(rows):
    if not rows:
        return pd.DataFrame(columns=["id", "lat", "lon", "source"])

    df = pd.DataFrame(rows)

    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")
    df["has_coords"] = df["lat"].notnull() & df["lon"].notnull()

    source_priority = {
        "cache": 0,
        "google": 1,
        "google_override": 1,
        "nominatim_struct": 2,
        "nominatim_structured": 2,
        "nominatim": 2,
        "fallback_cidade": 10,
        "falha": 99,
    }

    df["source_priority"] = df["source"].map(source_priority).fillna(50)

    df = (
        df.sort_values(
            by=["id", "has_coords", "source_priority"],
            ascending=[True, False, True],
        )
        .drop_duplicates(subset=["id"], keep="first")
        .copy()
    )

    return df[["id", "lat", "lon", "source"]]


def _merge_original(addresses, df_result):
    df_addresses = pd.DataFrame(addresses).copy()

    if df_addresses.empty:
        return pd.DataFrame(columns=["id", "lat", "lon", "source"])

    df_addresses["id"] = df_addresses["id"].astype(str)

    if df_result.empty:
        df_addresses["lat"] = None
        df_addresses["lon"] = None
        df_addresses["source"] = "falha"
        return df_addresses

    df_result = df_result.copy()
    df_result["id"] = df_result["id"].astype(str)

    return df_addresses.merge(df_result, on="id", how="left")


def _validar_resultados(df, gdf_municipios=None):
    if df.empty:
        return df, df

    df = df.copy()
    df["lat"] = pd.to_numeric(df["lat"], errors="coerce")
    df["lon"] = pd.to_numeric(df["lon"], errors="coerce")

    df_validos = df[df["lat"].notnull() & df["lon"].notnull()].copy()
    df_invalidos = df[df["lat"].isnull() | df["lon"].isnull()].copy()

    if not df_validos.empty:
        if gdf_municipios is None:
            gdf_municipios = carregar_municipios_gdf()
        df_validos = validar_municipios_batch_fast(df_validos, gdf_municipios)

        fora = df_validos[~df_validos["valido_municipio"]].copy()
        fora["motivo_invalidacao"] = "fora_municipio"

        df_validos = df_validos[df_validos["valido_municipio"]].copy()
        df_invalidos = pd.concat([df_invalidos, fora], ignore_index=True)

    if not df_invalidos.empty:
        df_invalidos["motivo_invalidacao"] = df_invalidos.get(
            "motivo_invalidacao",
            pd.Series(index=df_invalidos.index, dtype=object),
        ).fillna("falha")

    return df_validos, df_invalidos


def _reprocessar_invalidos(df_invalidos):
    if df_invalidos.empty:
        return pd.DataFrame(), df_invalidos

    df_reprocess = df_invalidos.copy()
    if "motivo_invalidacao" not in df_reprocess.columns:
        df_reprocess["motivo_invalidacao"] = "falha"

    service = ReprocessInvalidsService(database_writer=None)
    df_recuperados, df_mantidos = service.execute(df_reprocess)

    return df_recuperados, df_mantidos


def _revalidar_recuperados_com_regra_principal(df_recuperados, gdf_municipios):
    if df_recuperados.empty:
        return pd.DataFrame(), pd.DataFrame()

    if "id" not in df_recuperados.columns:
        raise Exception("[ERRO CRITICO] df_recuperados sem coluna 'id'")

    df_recuperados = df_recuperados.copy()
    df_recuperados["id"] = df_recuperados["id"].astype(str)

    df_fallback = df_recuperados[
        df_recuperados["source"].astype(str) == "fallback_cidade"
    ].copy()

    df_nao_fallback = df_recuperados[
        df_recuperados["source"].astype(str) != "fallback_cidade"
    ].copy()

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

    if not df_nao_fallback_invalidos.empty:
        logger.warning("[BATCH_JSON][GOOGLE_FORA_POLIGONO] -> fallback cidade")

        df_fallback_extra = df_nao_fallback_invalidos.copy()

        for idx, row in df_fallback_extra.iterrows():
            lat_fb, lon_fb = geocode_google_direto(
                f"{row.get('cidade')}, {row.get('uf')}, Brasil"
            )

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

    if not df_fallback.empty:
        df_fallback = validar_municipios_batch_fast(df_fallback, gdf_municipios)
        df_fallback_validos = df_fallback[df_fallback["valido_municipio"]].copy()
        df_fallback_invalidos = df_fallback[~df_fallback["valido_municipio"]].copy()
    else:
        df_fallback_validos = pd.DataFrame()
        df_fallback_invalidos = pd.DataFrame()

    df_recuperados_validos = pd.concat(
        [
            df_nao_fallback_validos,
            df_fallback_validos,
            df_fallback_extra_validos,
        ],
        ignore_index=True,
    )

    df_recuperados_invalidos = pd.concat(
        [
            df_nao_fallback_invalidos,
            df_fallback_invalidos,
            df_fallback_extra_invalidos,
        ],
        ignore_index=True,
    )

    if not df_recuperados_invalidos.empty:
        df_recuperados_invalidos = df_recuperados_invalidos.copy()
        if "motivo_invalidacao" not in df_recuperados_invalidos.columns:
            df_recuperados_invalidos["motivo_invalidacao"] = None
        df_recuperados_invalidos["motivo_invalidacao"] = (
            df_recuperados_invalidos["motivo_invalidacao"].fillna("fora_municipio")
        )

    return df_recuperados_validos, df_recuperados_invalidos


def _persistir_cache_final(df_validos_final: pd.DataFrame) -> int:
    if df_validos_final is None or df_validos_final.empty:
        return 0

    conn = None

    try:
        reader = DatabaseReader()
        conn = reader.conn
        writer = DatabaseWriter(conn)

        df_cache = df_validos_final.copy()

        if "valido_municipio" in df_cache.columns:
            df_cache = df_cache[df_cache["valido_municipio"]].copy()

        if df_cache.empty:
            return 0

        df_cache = df_cache.drop_duplicates(
            subset=["logradouro", "numero", "cidade", "uf"]
        )

        logger.info(f"[BATCH_JSON][CACHE_FINAL] candidatos={len(df_cache)}")

        saved = 0

        for _, row in df_cache.iterrows():
            if str(row.get("source") or "") == "fallback_cidade":
                continue

            logradouro = row.get("logradouro")
            numero = row.get("numero")
            cidade = row.get("cidade")
            uf = row.get("uf")
            lat = row.get("lat")
            lon = row.get("lon")

            if not logradouro or not numero or not cidade or not uf:
                continue

            if pd.isna(lat) or pd.isna(lon):
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
                    origem=row.get("source"),
                )
                saved += 1
            except Exception as e:
                logger.warning(f"[BATCH_JSON][CACHE_FINAL][ERRO] {e}")

        logger.info(f"[BATCH_JSON][CACHE_FINAL] salvos={saved}")
        return saved
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


def _to_api_results(df_validos, df_invalidos):
    results = []

    if not df_validos.empty:
        for _, row in df_validos.iterrows():
            results.append({
                "id": int(row["id"]),
                "lat": float(row["lat"]),
                "lon": float(row["lon"]),
                "source": row.get("source") or "ok",
                "valid": True,
            })

    if not df_invalidos.empty:
        for _, row in df_invalidos.iterrows():
            results.append({
                "id": int(row["id"]),
                "lat": None,
                "lon": None,
                "source": row.get("motivo_invalidacao") or row.get("source") or "falha",
                "valid": False,
            })

    results.sort(key=lambda r: r["id"])
    return results


def processar_batch_json(payload: dict):
    job = get_current_job()
    job_id = str(job.id) if job and job.id else str(uuid4())
    started_at = time.time()

    addresses = payload.get("addresses") or []
    tenant_id = int(payload.get("tenant_id", 1))
    origem = str(payload.get("origem", "api_batch_json"))

    default_chunk_size = int(os.getenv("GEOCODE_BATCH_JSON_CHUNK_SIZE", "100"))
    chunk_size = int(payload.get("chunk_size") or default_chunk_size)
    subjob_timeout = int(os.getenv("GEOCODE_SUBJOB_TIMEOUT", "1800"))
    total_chunks = (len(addresses) + chunk_size - 1) // chunk_size if addresses else 0

    redis_results_key = f"geocode_result:{job_id}"
    redis_done_key = f"geocode_done:{job_id}"
    redis_stats_key = f"geocode_stats:{job_id}"
    redis_final_key = f"geocode_batch_json_final:{job_id}"

    logger.info(
        f"[BATCH_JSON][START] job_id={job_id} total={len(addresses)} chunks={total_chunks}"
    )

    redis_str.delete(redis_results_key)
    redis_str.delete(redis_done_key)
    redis_str.delete(redis_stats_key)
    redis_str.delete(redis_final_key)

    _set_job_progress(job, 0, "Preparando seu processamento")

    if not addresses:
        result_payload = {
            "job_id": job_id,
            "status": "finished",
            "results": [],
            "stats": {"total": 0, "sucesso": 0, "falhas": 0, "tempo_ms": 0},
        }
        redis_str.set(redis_final_key, json.dumps(result_payload, ensure_ascii=False), ex=86400)
        return result_payload

    _set_job_progress(job, 5, "Carregando malha municipal")
    stop_loading_heartbeat = _start_progress_heartbeat(
        job,
        start=6,
        end=18,
        step="Validando municipios e cidades",
        interval=4.0,
    )
    try:
        gdf_municipios = carregar_municipios_gdf()
        addresses_validas, df_invalidos_cidade = _separar_cidades_invalidas(addresses, gdf_municipios)
    finally:
        stop_loading_heartbeat()

    _set_job_progress(job, 18, "Cidades validadas")

    if not df_invalidos_cidade.empty:
        logger.warning(f"[BATCH_JSON][CIDADE_INVALIDA] total={len(df_invalidos_cidade)}")

    total_chunks = (
        (len(addresses_validas) + chunk_size - 1) // chunk_size if addresses_validas else 0
    )

    subjob_queue = Queue("geocode_subjobs", connection=redis_conn)
    subjob_ids = {}
    chunk_sizes = {}

    for chunk_id, chunk in enumerate(_chunk_list(addresses_validas, chunk_size)):
        subjob = subjob_queue.enqueue(
            "geocoding_engine.workers.geocode_subjob.processar_subjob",
            {
                "chunk_id": chunk_id,
                "parent_id": job_id,
                "addresses": chunk,
                "tenant_id": tenant_id,
                "origem": origem,
            },
            job_timeout=subjob_timeout,
            retry=Retry(max=1, interval=[5]),
        )
        subjob_ids[chunk_id] = subjob.id
        chunk_sizes[chunk_id] = len(chunk)

    _set_job_progress(job, 20, "Distribuindo lotes para geocodificacao")

    while True:
        done_chunks = {
            int(chunk_id)
            for chunk_id in redis_str.smembers(redis_done_key)
            if str(chunk_id).isdigit()
        }
        finished_chunks = len(done_chunks)

        for chunk_id, subjob_id in subjob_ids.items():
            if chunk_id in done_chunks:
                continue

            try:
                subjob = Job.fetch(subjob_id, connection=redis_conn)
                status = subjob.get_status(refresh=True)
            except Exception as e:
                logger.warning(
                    f"[BATCH_JSON][SUBJOB_STATUS_FAIL] job_id={job_id} "
                    f"chunk_id={chunk_id} subjob_id={subjob_id} erro={e}"
                )
                continue

            if status == "failed":
                error = subjob.exc_info or "subjob_failed"
                logger.error(
                    f"[BATCH_JSON][CHUNK_FAILED] job_id={job_id} "
                    f"chunk_id={chunk_id} subjob_id={subjob_id}"
                )
                _mark_chunk_failed(
                    redis_results_key,
                    redis_done_key,
                    redis_stats_key,
                    chunk_id,
                    chunk_sizes.get(chunk_id, 0),
                    error,
                )
                done_chunks.add(chunk_id)
                finished_chunks = len(done_chunks)

        progress = 10 + int((finished_chunks / total_chunks) * 55) if total_chunks else 65
        progress = min(progress, 65)

        _set_job_progress(
            job,
            progress,
            f"Localizando enderecos ({finished_chunks}/{total_chunks})",
        )

        if finished_chunks >= total_chunks:
            break

        time.sleep(1)

    _set_job_progress(job, 70, "Organizando os resultados")

    raw_items = redis_str.lrange(redis_results_key, 0, -1)
    _set_job_progress(job, 74, "Consolidando respostas recebidas")

    df_result = _dedup_results(_coerce_result_rows(raw_items))
    _set_job_progress(job, 78, "Associando resultados aos enderecos")

    df_merged = _merge_original(addresses_validas, df_result)

    _set_job_progress(job, 82, "Validando coordenadas retornadas")
    df_validos, df_invalidos = _validar_resultados(df_merged, gdf_municipios)

    _set_job_progress(job, 86, "Fazendo uma nova conferencia dos casos pendentes")

    stop_reprocess_heartbeat = _start_progress_heartbeat(
        job,
        start=87,
        end=91,
        step="Reavaliando casos pendentes",
        interval=4.0,
    )
    try:
        df_recuperados, df_invalidos_final = _reprocessar_invalidos(df_invalidos)

        df_recuperados_validos, df_recuperados_invalidos = _revalidar_recuperados_com_regra_principal(
            df_recuperados,
            gdf_municipios,
        )
    finally:
        stop_reprocess_heartbeat()

    _set_job_progress(job, 91, "Validando recuperacoes aplicadas")

    df_validos_final = pd.concat([df_validos, df_recuperados_validos], ignore_index=True)
    if not df_validos_final.empty:
        df_validos_final = df_validos_final.drop_duplicates(subset=["id"], keep="first")

    ids_validos = set(df_validos_final["id"].astype(str)) if not df_validos_final.empty else set()

    df_invalidos_final = pd.concat(
        [
            df_invalidos_cidade,
            df_invalidos_final,
            df_recuperados_invalidos,
        ],
        ignore_index=True,
    )
    if not df_invalidos_final.empty:
        df_invalidos_final["id"] = df_invalidos_final["id"].astype(str)
        df_invalidos_final = df_invalidos_final[
            ~df_invalidos_final["id"].isin(ids_validos)
        ].drop_duplicates(subset=["id"], keep="first")

    _set_job_progress(job, 94, "Aplicando os ajustes finais")

    stop_finalize_heartbeat = _start_progress_heartbeat(
        job,
        start=95,
        end=99,
        step="Gravando ajustes finais",
        interval=3.0,
    )
    try:
        cache_saved = _persistir_cache_final(df_validos_final)
    finally:
        stop_finalize_heartbeat()

    results = _to_api_results(df_validos_final, df_invalidos_final)
    stats_live = _get_hash_stats(redis_stats_key)
    tempo_ms = int((time.time() - started_at) * 1000)

    stats = {
        "total": len(addresses),
        "sucesso": len(df_validos_final),
        "falhas": len(df_invalidos_final),
        "cache_hits": stats_live["cache_hits"],
        "nominatim_hits": stats_live["nominatim_hits"],
        "google_hits": stats_live["google_hits"],
        "chunks_done": stats_live["chunks_done"],
        "chunks_failed": stats_live["chunks_failed"],
        "cache_saved": cache_saved,
        "tempo_ms": tempo_ms,
    }

    result_payload = {
        "job_id": job_id,
        "status": "finished",
        "results": results,
        "stats": stats,
    }

    redis_str.set(redis_final_key, json.dumps(result_payload, ensure_ascii=False), ex=86400)

    _set_job_progress(
        job,
        100,
        "Processamento concluido",
        status="finished",
        result=stats,
        result_key=redis_final_key,
    )

    logger.info(
        f"[BATCH_JSON][END] job_id={job_id} total={stats['total']} "
        f"sucesso={stats['sucesso']} falhas={stats['falhas']} tempo_ms={tempo_ms}"
    )

    return result_payload

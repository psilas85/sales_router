#sales_router/src/geocoding_engine/workers/geocode_subjob.py

import json
import logging
import os
import sys
import time
import gc

import redis
from rq import get_current_job

from geocoding_engine.application.geocode_addresses_use_case import GeocodeAddressesUseCase

logger = logging.getLogger("geocode_subjob")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)

r = redis.Redis(host="redis", port=6379, decode_responses=True)


def _safe_rpush(redis_key: str, payload: dict, attempts: int = 3, sleep_s: float = 0.5):
    last_err = None

    for attempt in range(1, attempts + 1):
        try:
            r.rpush(redis_key, json.dumps(payload, ensure_ascii=False))
            return
        except Exception as e:
            last_err = e
            logger.warning(f"[REDIS][RPUSH][ERRO] tentativa={attempt}/{attempts} erro={e}")
            time.sleep(sleep_s)

    raise last_err


def _safe_sadd(done_key: str, chunk_id: int, attempts: int = 3, sleep_s: float = 0.5):
    last_err = None

    for attempt in range(1, attempts + 1):
        try:
            r.sadd(done_key, str(chunk_id))
            return
        except Exception as e:
            last_err = e
            logger.warning(f"[REDIS][SADD][ERRO] tentativa={attempt}/{attempts} erro={e}")
            time.sleep(sleep_s)

    raise last_err


def _safe_hincrby(stats_key: str, field: str, value: int, attempts: int = 3, sleep_s: float = 0.5):
    last_err = None

    for attempt in range(1, attempts + 1):
        try:
            r.hincrby(stats_key, field, int(value))
            return
        except Exception as e:
            last_err = e
            logger.warning(
                f"[REDIS][HINCRBY][ERRO] field={field} tentativa={attempt}/{attempts} erro={e}"
            )
            time.sleep(sleep_s)

    raise last_err


def processar_subjob(payload: dict):
    job = get_current_job()

    chunk_id = int(payload["chunk_id"])
    parent_id = str(payload["parent_id"])
    addresses = payload["addresses"]
    tenant_id = payload.get("tenant_id", 1)
    origem = payload.get("origem", "batch_subjob")

    redis_results_key = f"geocode_result:{parent_id}"
    redis_done_key = f"geocode_done:{parent_id}"
    redis_stats_key = f"geocode_stats:{parent_id}"

    logger.info(
        f"🚀 Subjob iniciado | subjob_id={job.id if job else None} "
        f"parent_id={parent_id} chunk_id={chunk_id} total={len(addresses)}"
    )

    total_processados = 0
    total_falhas = 0
    total_cache_hits = 0
    total_nominatim_hits = 0
    total_google_hits = 0

    # Evita reprocessar retry duplicando sinal de conclusão
    try:
        if r.sismember(redis_done_key, str(chunk_id)):
            logger.warning(
                f"[SUBJOB][JA_CONCLUIDO] parent_id={parent_id} chunk_id={chunk_id} - ignorando retry"
            )
            return {
                "status": "already_done",
                "chunk_id": chunk_id,
                "total": 0
            }
    except Exception as e:
        logger.warning(f"[REDIS][SISMEMBER][ERRO] chunk_id={chunk_id} erro={e}")

    try:
        uc = GeocodeAddressesUseCase()

        # Micro-batch moderado: reduz overhead sem carregar o chunk inteiro em memória.
        batch_interno = int(os.getenv("GEOCODE_SUBJOB_MICROBATCH_SIZE", "10"))

        for i in range(0, len(addresses), batch_interno):
            batch = addresses[i:i + batch_interno]

            result = uc.execute(
                batch,
                tenant_id=tenant_id,
                origem=origem,
                persist_cache=False,
                validate_polygon=False,
            )

            results = result.get("results", [])
            stats = result.get("stats", {})

            if not isinstance(results, list):
                logger.warning(f"[SUBJOB][RESULT_INVALIDO] chunk={chunk_id} tipo={type(results)}")
                results = []

            # Sanitização forte
            # =====================================================
            # SANITIZAÇÃO FORTE (CORREÇÃO DEFINITIVA)
            # =====================================================

            import math

            sanitized_results = []

            for r_item in results:

                if not isinstance(r_item, dict):
                    continue

                lat = r_item.get("lat")
                lon = r_item.get("lon")

                logger.debug(
                    f"[SUBJOB_RAW] lat={lat} lon={lon} tipo_lat={type(lat)} tipo_lon={type(lon)}"
                )

                lat_ok = (
                    isinstance(lat, (int, float)) and
                    not math.isnan(lat) and
                    not math.isinf(lat)
                )

                lon_ok = (
                    isinstance(lon, (int, float)) and
                    not math.isnan(lon) and
                    not math.isinf(lon)
                )

                if lat_ok and lon_ok:
                    sanitized_results.append({
                        "id": str(r_item.get("id")),
                        "lat": float(lat),
                        "lon": float(lon),
                        "source": r_item.get("source")
                    })
                else:
                    # 🔥 força falha explícita
                    sanitized_results.append({
                        "id": str(r_item.get("id")),
                        "lat": None,
                        "lon": None,
                        "source": "falha"
                    })

                logger.debug(
                    f"[SUBJOB_FINAL] lat={sanitized_results[-1]['lat']} lon={sanitized_results[-1]['lon']}"
                )
            total_processados += len(sanitized_results)
            total_falhas += int(stats.get("falhas", 0))
            total_cache_hits += int(stats.get("cache_hits", 0))
            total_nominatim_hits += int(stats.get("nominatim_hits", 0))
            total_google_hits += int(stats.get("google_hits", 0))

            payload_redis = {
                "chunk_id": chunk_id,
                "microbatch_index": i // batch_interno,
                "results": sanitized_results,
                "stats": {
                    "total": int(stats.get("total", len(batch))),
                    "cache_hits": int(stats.get("cache_hits", 0)),
                    "nominatim_hits": int(stats.get("nominatim_hits", 0)),
                    "google_hits": int(stats.get("google_hits", 0)),
                    "falhas": int(stats.get("falhas", 0)),
                    "tempo_ms": int(stats.get("tempo_ms", 0))
                }
            }

            _safe_rpush(redis_results_key, payload_redis)

            logger.info(
                f"[SUBJOB][MICROBATCH] parent_id={parent_id} chunk_id={chunk_id} "
                f"microbatch={i // batch_interno} results={len(sanitized_results)} "
                f"falhas={stats.get('falhas', 0)}"
            )

            del result
            del results
            del stats
            del sanitized_results
            del payload_redis
            gc.collect()

        # Agregados do chunk
        _safe_hincrby(redis_stats_key, "cache_hits", total_cache_hits)
        _safe_hincrby(redis_stats_key, "nominatim_hits", total_nominatim_hits)
        _safe_hincrby(redis_stats_key, "google_hits", total_google_hits)
        _safe_hincrby(redis_stats_key, "falhas", total_falhas)
        _safe_hincrby(redis_stats_key, "chunks_done", 1)
        _safe_hincrby(redis_stats_key, "results_count", total_processados)

        # Sinal de conclusão do chunk: só no fim
        _safe_sadd(redis_done_key, chunk_id)

    except Exception as e:
        logger.error(f"[SUBJOB][ERRO] chunk={chunk_id} erro={e}")

        erro_payload = {
            "chunk_id": chunk_id,
            "microbatch_index": -1,
            "results": [],
            "stats": {
                "total": len(addresses),
                "cache_hits": 0,
                "nominatim_hits": 0,
                "google_hits": 0,
                "falhas": len(addresses),
                "tempo_ms": 0
            },
            "error": str(e)
        }

        try:
            _safe_rpush(redis_results_key, erro_payload)
            _safe_hincrby(redis_stats_key, "falhas", len(addresses))
            _safe_hincrby(redis_stats_key, "chunks_failed", 1)
            _safe_sadd(redis_done_key, chunk_id)
        except Exception as inner:
            logger.error(f"[SUBJOB][ERRO_FINALIZACAO] chunk={chunk_id} erro={inner}")

    finally:
        del addresses
        gc.collect()

    logger.info(
        f"✅ Subjob finalizado | parent_id={parent_id} chunk_id={chunk_id} "
        f"processados={total_processados} falhas={total_falhas} "
        f"cache={total_cache_hits} nominatim={total_nominatim_hits} google={total_google_hits}"
    )

    return {
        "status": "done",
        "chunk_id": chunk_id,
        "total": total_processados
    }

#sales_router/src/geocoding_engine/workers/geocode_subjob.py

import json
import logging
import sys

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


def processar_subjob(payload: dict):

    job = get_current_job()

    chunk_id = payload["chunk_id"]
    parent_id = payload["parent_id"]
    addresses = payload["addresses"]
    tenant_id = payload.get("tenant_id", 1)
    origem = payload.get("origem", "batch_subjob")

    logger.info(
        f"🚀 Subjob iniciado | parent_id={parent_id} chunk_id={chunk_id} total={len(addresses)}"
    )

    uc = GeocodeAddressesUseCase()
    result = uc.execute(addresses, tenant_id=tenant_id, origem="subjob")

    redis_key = f"geocode_result:{parent_id}"

    r.rpush(redis_key, json.dumps({
        "chunk_id": chunk_id,
        "results": result["results"]
    }, ensure_ascii=False))

    logger.info(
        f"✅ Subjob finalizado | parent_id={parent_id} chunk_id={chunk_id} total={len(result['results'])}"
    )

    return {
        "status": "done",
        "chunk_id": chunk_id,
        "total": len(result["results"])
    }
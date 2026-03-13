#sales_router/src/geocoding_engine/api/routes.py

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from .dependencies import verify_token
from .schemas import GeocodeRequest
from fastapi.responses import FileResponse

from geocoding_engine.application.geocode_addresses_use_case import GeocodeAddressesUseCase
from geocoding_engine.infrastructure.queue_factory import fila_geocode, redis_conn
from geocoding_engine.workers.geocode_jobs import processar_geocode


from rq.job import Job
from loguru import logger
import os
import uuid
from redis import Redis

import tempfile


router = APIRouter()

MAX_UPLOAD_MB = 50


# ============================================================
# HEALTH
# ============================================================

@router.get("/health")
def health():
    return {"status": "ok", "service": "geocoding_engine"}


# ============================================================
# GEOCODE JSON
# ============================================================

@router.post(
    "/geocode",
    dependencies=[Depends(verify_token)]
)
def geocode_json(body: GeocodeRequest):

    endereco = f"{body.endereco}, {body.cidade} - {body.uf}"

    logger.info(f"[GEOCODE] request recebido: {endereco}")

    uc = GeocodeAddressesUseCase()

    res = uc.execute([
        {
            "id": 1,
            "address": endereco
        }
    ])

    r = res["results"][0]

    logger.info(
        f"[GEOCODE] resultado lat={r['lat']} lon={r['lon']} source={r['source']}"
    )

    return {
        "lat": r["lat"],
        "lon": r["lon"],
        "status": "ok" if r["lat"] else "not_found"
    }


# ============================================================
# GEOCODE PLANILHA (ASSÍNCRONO)
# ============================================================

@router.post(
    "/upload",
    dependencies=[Depends(verify_token)]
)
async def geocode_upload(file: UploadFile = File(...)):

    logger.info(f"[UPLOAD] arquivo recebido: {file.filename}")

    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Arquivo deve ser XLSX")

    file_bytes = await file.read()

    if len(file_bytes) > MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail=f"Arquivo muito grande (máx {MAX_UPLOAD_MB}MB)"
        )

    # diretório compartilhado entre containers
    UPLOAD_DIR = "/app/data/uploads"
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    file_id = str(uuid.uuid4())
    file_path = f"{UPLOAD_DIR}/{file_id}.xlsx"

    with open(file_path, "wb") as f:
        f.write(file_bytes)

    logger.info(f"[UPLOAD] salvo: {file_path}")

    queue = fila_geocode()

    job = queue.enqueue(
        processar_geocode,
        file_path,
        job_timeout=36000
    )

    logger.info(
        f"[UPLOAD] job criado: {job.id} | file={file.filename}"
    )

    return {
        "status": "queued",
        "job_id": job.id
    }

# ============================================================
# JOB STATUS
# ============================================================

@router.get(
    "/job/{job_id}",
    dependencies=[Depends(verify_token)]
)
def job_status(job_id: str):

    try:
        job = Job.fetch(job_id, connection=redis_conn)

    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    response = {
        "job_id": job.id,
        "status": job.get_status(),
        "progress": job.meta.get("progress"),
        "step": job.meta.get("step"),
    }

    if job.is_finished:
        response["result"] = job.result

    if job.is_failed:
        response["error"] = str(job.exc_info)

    return response

@router.get(
    "/job/{job_id}/download",
    dependencies=[Depends(verify_token)]
)
def download_result(job_id: str):

    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.get_status() != "finished":
        raise HTTPException(
            status_code=400,
            detail="Job ainda não finalizado"
        )

    output_file = job.meta.get("output_file")

    if not output_file or not os.path.exists(output_file):
        raise HTTPException(
            status_code=404,
            detail="Arquivo de resultado não encontrado"
        )

    return FileResponse(
        output_file,
        filename=f"geocode_{job_id}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
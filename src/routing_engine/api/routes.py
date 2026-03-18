#sales_router/src/routing_engine/api/routes.py

# routing_engine/api/routes.py

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from routing_engine.api.dependencies import verify_token

from routing_engine.infrastructure.queue_factory import get_queue
from routing_engine.workers.routing_jobs import processar_routing

from rq.job import Job
from redis import Redis

import os
import uuid

router = APIRouter()

MAX_UPLOAD_MB = 50


# ============================================================
# HEALTH
# ============================================================

@router.get("/health")
def health():
    return {"status": "ok", "service": "routing_engine"}


# ============================================================
# UPLOAD (ASSÍNCRONO)
# ============================================================

@router.post(
    "/upload",
    dependencies=[Depends(verify_token)]
)
async def routing_upload(file: UploadFile = File(...)):

    if not file.filename.endswith(".xlsx"):
        raise HTTPException(status_code=400, detail="Arquivo deve ser XLSX")

    file_bytes = await file.read()

    if len(file_bytes) > MAX_UPLOAD_MB * 1024 * 1024:
        raise HTTPException(
            status_code=400,
            detail=f"Arquivo muito grande (máx {MAX_UPLOAD_MB}MB)"
        )

    UPLOAD_DIR = "/app/data/uploads"
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    file_id = str(uuid.uuid4())
    file_path = f"{UPLOAD_DIR}/{file_id}.xlsx"

    with open(file_path, "wb") as f:
        f.write(file_bytes)

    queue = get_queue("routing_jobs")

    job = queue.enqueue(
        processar_routing,
        file_path,
        job_timeout=36000
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

    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    conn = Redis.from_url(redis_url)

    try:
        job = Job.fetch(job_id, connection=conn)
    except:
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


# ============================================================
# DOWNLOAD
# ============================================================

@router.get(
    "/job/{job_id}/download",
    dependencies=[Depends(verify_token)]
)
def download_result(job_id: str):

    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    conn = Redis.from_url(redis_url)

    try:
        job = Job.fetch(job_id, connection=conn)
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
            detail="Arquivo não encontrado"
        )

    return FileResponse(
        output_file,
        filename=f"routing_{job_id}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )
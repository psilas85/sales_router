#sales_router/src/routing_engine/api/routes.py

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Form
from fastapi.responses import FileResponse, JSONResponse
from routing_engine.api.dependencies import verify_token

from routing_engine.infrastructure.queue_factory import get_queue
from routing_engine.workers.routing_jobs import processar_routing
from routing_engine.visualization.map_use_case import GenerateMapUseCase

from rq.job import Job
from redis import Redis
import os
import uuid
import json

router = APIRouter()

MAX_UPLOAD_MB = 50


# ============================================================
# REDIS HELPER
# ============================================================

def get_redis_conn():
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    return Redis.from_url(redis_url)


# ============================================================
# HEALTH
# ============================================================

@router.get("/health")
def health():
    return {"status": "ok", "service": "routing_engine"}


# ============================================================
# UPLOAD
# ============================================================

@router.post(
    "/upload",
    dependencies=[Depends(verify_token)]
)
async def routing_upload(
    file: UploadFile = File(...),
    params: str | None = Form(None)
):

        if not file.filename.endswith(".xlsx"):
            raise HTTPException(status_code=400, detail="Arquivo deve ser XLSX")

        file_bytes = await file.read()

        if len(file_bytes) > MAX_UPLOAD_MB * 1024 * 1024:
            raise HTTPException(
                status_code=400,
                detail=f"Arquivo muito grande (máx {MAX_UPLOAD_MB}MB)"
            )

        try:
            parsed_params = json.loads(params) if params else {}
        except Exception:
            raise HTTPException(400, "Parâmetros inválidos")

        UPLOAD_DIR = "/app/data/uploads"
        os.makedirs(UPLOAD_DIR, exist_ok=True)

        file_id = str(uuid.uuid4())
        file_path = f"{UPLOAD_DIR}/{file_id}.xlsx"

        with open(file_path, "wb") as f:
            f.write(file_bytes)

        queue = get_queue("routing_batch")

        job = queue.enqueue(
            processar_routing,
            file_path,
            parsed_params,
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

    conn = get_redis_conn()

    try:
        job = Job.fetch(job_id, connection=conn)
    except Exception:
        return {
            "status": "not_found"
        }

    if not job:
        return {
            "status": "not_found"
        }

    response = {
        "job_id": job.id,
        "status": job.get_status(),
        "progress": job.meta.get("progress"),
        "step": job.meta.get("step"),
        "summary": job.meta.get("summary")
    }

    # ✔ sucesso
    if job.is_finished:
        response["result"] = job.result

    # ✔ erro (SIMPLES E CORRETO)
    if job.is_failed:
        response["error"] = job.meta.get("error") or {
            "mensagem": "Erro desconhecido no processamento"
        }

    return response

# ============================================================
# DOWNLOAD
# ============================================================

@router.get(
    "/job/{job_id}/download",
    dependencies=[Depends(verify_token)]
)
def download_result(job_id: str):

    conn = get_redis_conn()

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


# ============================================================
# MAP (PADRÃO CORRETO)
# ============================================================

@router.get(
    "/job/{job_id}/map",
    dependencies=[Depends(verify_token)]
)
def job_map(job_id: str):

    conn = get_redis_conn()

    try:
        job = Job.fetch(job_id, connection=conn)
    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.get_status() != "finished":
        raise HTTPException(
            status_code=400,
            detail="Job ainda não finalizado"
        )

    json_path = job.meta.get("output_json")

    if not json_path or not os.path.exists(json_path):
        raise HTTPException(
            status_code=404,
            detail="JSON não encontrado"
        )

    uc = GenerateMapUseCase()
    geojson = uc.execute(json_path)

    return JSONResponse(content=geojson)


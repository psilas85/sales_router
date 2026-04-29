#sales_router/src/routing_engine/api/routes.py

from typing import Any

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import FileResponse, JSONResponse
from routing_engine.api.dependencies import verify_token
from pydantic import BaseModel

from routing_engine.infrastructure.queue_factory import get_queue
from routing_engine.workers.routing_jobs import processar_routing
from routing_engine.application.prepared_groups_routing_use_case import executar_prepared_groups_master_job
from routing_engine.visualization.map_use_case import GenerateMapUseCase

from rq.job import Job
from redis import Redis
import os
import uuid
import json

router = APIRouter()

MAX_UPLOAD_MB = 50


class PreparedGroupPDV(BaseModel):
    pdv_id: int
    lat: float
    lon: float
    cidade: str | None = None
    uf: str | None = None
    freq_visita: float | None = None
    cnpj: str | None = None
    nome_fantasia: str | None = None
    logradouro: str | None = None
    numero: str | None = None
    bairro: str | None = None
    cep: str | None = None
    grupo_utilizado: str | None = None
    fonte_grupo: str | None = None


class PreparedGroup(BaseModel):
    group_id: str
    group_type: str = "cluster"
    cluster_id: int | None = None
    run_id: int | None = None
    centro_lat: float
    centro_lon: float
    pdvs: list[PreparedGroupPDV]


class PreparedGroupsParams(BaseModel):
    modo_calculo: str = "frequencia"
    dias_uteis: int
    frequencia_visita: float
    min_pdvs_rota: int
    max_pdvs_rota: int
    service_min: float = 30.0
    v_kmh: float = 60.0
    alpha_path: float = 1.3
    twoopt: bool = False
    preserve_sequence: bool = False


class PreparedGroupsRequest(BaseModel):
    tenant_id: int
    routing_id: str
    parent_job_id: str | None = None
    requested_by: str | None = None
    mode: str = "balanceado"
    params: PreparedGroupsParams
    groups: list[PreparedGroup]


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


@router.post(
    "/internal/prepared-groups/balanced-routing",
    dependencies=[Depends(verify_token)]
)
def prepared_groups_balanced_routing(
    body: PreparedGroupsRequest,
    request: Request,
):
    user = request.state.user
    user_tenant_id = user.get("tenant_id")

    if user_tenant_id is not None and int(user_tenant_id) != body.tenant_id:
        raise HTTPException(status_code=403, detail="tenant_id divergente do token")

    if body.mode != "balanceado":
        raise HTTPException(status_code=400, detail="Este endpoint aceita apenas modo balanceado")

    if not body.groups:
        raise HTTPException(status_code=400, detail="Nenhum grupo informado")

    queue = get_queue("routing_prepared_groups")
    job = queue.enqueue(
        executar_prepared_groups_master_job,
        body.dict(),
        job_timeout=36000,
    )

    job.meta.update({
        "progress": 0,
        "step": "Job enfileirado",
        "routing_id": body.routing_id,
        "summary": {
            "groups_received": len(body.groups),
        },
    })
    job.save_meta()

    return {
        "status": "queued",
        "job_id": job.id,
        "routing_id": body.routing_id,
    }


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


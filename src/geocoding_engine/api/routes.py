#sales_router/src/geocoding_engine/api/routes.py

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.encoders import jsonable_encoder

from .dependencies import verify_token
from .schemas import GeocodeRequest

from geocoding_engine.application.geocode_addresses_use_case import GeocodeAddressesUseCase
from geocoding_engine.infrastructure.queue_factory import fila_geocode, redis_conn
from geocoding_engine.workers.geocode_jobs import processar_geocode
from geocoding_engine.visualization.map_use_case import GenerateMapUseCase
from geocoding_engine.infrastructure.database_reader import DatabaseReader
from geocoding_engine.infrastructure.database_writer import DatabaseWriter

from rq.job import Job
from loguru import logger

import uuid
import os
import json

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

    res = uc.execute([{
        "id": 1,
        "address": endereco,
        "cidade": body.cidade,
        "uf": body.uf,
        "cep": getattr(body, "cep", None)  # opcional
    }])

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

    UPLOAD_DIR = "/app/data/uploads"
    os.makedirs(UPLOAD_DIR, exist_ok=True)

    file_id = str(uuid.uuid4())
    file_path = f"{UPLOAD_DIR}/{file_id}.xlsx"

    try:
        with open(file_path, "wb") as f:
            f.write(file_bytes)

        logger.info(f"[UPLOAD] salvo: {file_path}")

        queue = fila_geocode()

        job = queue.enqueue(
            processar_geocode,
            file_path,
            job_timeout=36000,
            meta={
                "progress": 0,
                "step": "Criado"
            },
            description=f"geocode:{file.filename}"
        )

        logger.info(
            f"[UPLOAD] job criado: {job.id} | file={file.filename}"
        )

        return {
            "status": "queued",
            "job_id": job.id
        }

    except Exception as e:
        if os.path.exists(file_path):
            os.remove(file_path)
        logger.error(f"[UPLOAD][ERRO] {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao criar job: {e}")


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
        "progress": job.meta.get("progress", 0),
        "step": job.meta.get("step", "N/A"),
    }

    if job.is_finished:
        result = job.result
        response["result"] = result

        if isinstance(result, dict):
            response["total"] = result.get("total")
            response["sucesso"] = result.get("sucesso")
            response["falhas"] = result.get("falhas")

    if job.is_failed:
        response["error"] = str(job.exc_info)

    return response


# ============================================================
# DOWNLOAD RESULT
# ============================================================

@router.get(
    "/job/{job_id}/download",
    dependencies=[Depends(verify_token)]
)
def download_result(job_id: str):

    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
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


# ============================================================
# RESULT JSON (MAPA)
# ============================================================

@router.get(
    "/job/{job_id}/result",
    dependencies=[Depends(verify_token)]
)
def job_result(job_id: str):

    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.get_status() != "finished":
        raise HTTPException(
            status_code=400,
            detail="Job ainda não finalizado"
        )

    output_json = job.meta.get("output_json")

    if not output_json or not os.path.exists(output_json):
        raise HTTPException(
            status_code=404,
            detail="Arquivo JSON de resultado não encontrado"
        )

    # 🔥 CORREÇÃO AQUI
    import json
    from fastapi.responses import JSONResponse

    with open(output_json, "r", encoding="utf-8") as f:
        data = json.load(f)

    return JSONResponse(content=data)

# ============================================================
# MAPA GEOJSON
# ============================================================

@router.get(
    "/job/{job_id}/map",
    dependencies=[Depends(verify_token)]
)
def job_map(job_id: str):

    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.get_status() != "finished":
        raise HTTPException(
            status_code=400,
            detail="Job ainda não finalizado"
        )

    json_path = job.meta.get("output_json")

    if not json_path:
        raise HTTPException(404, "JSON não encontrado")

    geojson_path = json_path.replace(".json", "_geo.json")

    uc = GenerateMapUseCase()
    geojson = uc.execute(geojson_path)

    return JSONResponse(content=geojson)


# ============================================================
# CACHE SEARCH
# ============================================================

@router.get("/cache/search")
def buscar_cache(
    cidade: str,
    uf: str,
    endereco: str = None,
    limit: int = 50,
):
    reader = DatabaseReader()

    results = reader.buscar_cache_filtrado(
        cidade=cidade,
        uf=uf,
        endereco=endereco,
        limit=limit
    )

    return results


# ============================================================
# CACHE UPDATE
# ============================================================

@router.put("/cache/{id}")
def atualizar_cache(id: int, payload: dict):

    lat = payload.get("lat")
    lon = payload.get("lon")

    if lat is None or lon is None:
        raise HTTPException(400, "lat/lon obrigatórios")

    reader = DatabaseReader()
    writer = DatabaseWriter(reader.conn)

    writer.atualizar_cache(id, lat, lon)

    return {"status": "ok"}
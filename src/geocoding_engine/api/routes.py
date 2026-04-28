#sales_router/src/geocoding_engine/api/routes.py

from typing import Optional

from fastapi import APIRouter, Depends, UploadFile, File, HTTPException
from fastapi.responses import FileResponse, JSONResponse
from fastapi.encoders import jsonable_encoder
from pydantic import BaseModel, Field

from .dependencies import verify_token
from .schemas import GeocodeBatchRequest, GeocodeRequest

from geocoding_engine.application.geocode_addresses_use_case import GeocodeAddressesUseCase
from geocoding_engine.infrastructure.queue_factory import fila_geocode, redis_conn
from geocoding_engine.workers.geocode_batch_json_job import processar_batch_json
from geocoding_engine.workers.geocode_jobs import processar_geocode
from geocoding_engine.visualization.map_use_case import GenerateMapUseCase
from geocoding_engine.infrastructure.database_reader import DatabaseReader
from geocoding_engine.infrastructure.database_writer import DatabaseWriter
from geocoding_engine.domain.cache_key_builder import build_cache_key, build_canonical_address

from rq.job import Job
from loguru import logger

import uuid
import os
import json
import pandas as pd

router = APIRouter()

MAX_UPLOAD_MB = 50


class CacheCreateSchema(BaseModel):
    endereco: str = Field(..., min_length=5, max_length=255)
    logradouro: str = Field(..., min_length=2, max_length=200)
    numero: Optional[str] = Field(default="", max_length=40)
    cidade: str = Field(..., min_length=2, max_length=120)
    uf: str = Field(..., min_length=2, max_length=2)
    lat: float
    lon: float


class CacheUpdateSchema(BaseModel):
    endereco: Optional[str] = Field(default=None, min_length=5, max_length=255)
    logradouro: Optional[str] = Field(default=None, min_length=2, max_length=200)
    numero: Optional[str] = Field(default=None, max_length=40)
    cidade: Optional[str] = Field(default=None, min_length=2, max_length=120)
    uf: Optional[str] = Field(default=None, min_length=2, max_length=2)
    lat: float
    lon: float


def validar_lat_lon(lat: float, lon: float):
    if lat < -90 or lat > 90 or lon < -180 or lon > 180:
        raise HTTPException(status_code=400, detail="Latitude/longitude fora da faixa válida")


def montar_endereco_display(endereco: Optional[str], logradouro: str, numero: Optional[str], cidade: str, uf: str):
    if logradouro and cidade and uf:
        return build_canonical_address(logradouro, numero or "", cidade, uf)

    return (endereco or "").strip()


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

    logradouro = body.logradouro or body.endereco or body.address
    numero = body.numero or ""
    endereco_base = body.address or body.endereco or body.logradouro or ""

    if body.address:
        endereco = body.address
    else:
        partes_rua = " ".join(p for p in [logradouro or "", numero] if p).strip()
        bairro = f", {body.bairro}" if body.bairro else ""
        endereco = f"{partes_rua}{bairro}, {body.cidade} - {body.uf}"

    logger.info(f"[GEOCODE] request recebido: {endereco}")

    uc = GeocodeAddressesUseCase()

    res = uc.execute([{
        "id": body.id or 1,
        "address": endereco,
        "logradouro": logradouro or endereco_base,
        "numero": numero,
        "bairro": body.bairro,
        "cidade": body.cidade,
        "uf": body.uf,
        "cep": body.cep,
    }])

    if not res["results"]:
        return {
            "lat": None,
            "lon": None,
            "status": "not_found"
        }

    r = res["results"][0]

    logger.info(
        f"[GEOCODE] resultado lat={r['lat']} lon={r['lon']} source={r['source']}"
    )

    return {
        "lat": r["lat"],
        "lon": r["lon"],
        "status": "ok" if r["lat"] else "not_found"
    }


@router.post(
    "/geocode/batch",
    dependencies=[Depends(verify_token)]
)
def geocode_batch(body: GeocodeBatchRequest):

    payload = []

    for idx, item in enumerate(body.addresses):
        logradouro = item.logradouro or item.endereco or item.address
        numero = item.numero or ""

        if item.address:
            endereco = item.address
        else:
            partes_rua = " ".join(p for p in [logradouro or "", numero] if p).strip()
            bairro = f", {item.bairro}" if item.bairro else ""
            endereco = f"{partes_rua}{bairro}, {item.cidade} - {item.uf}"

        payload.append({
            "id": item.id if item.id is not None else idx,
            "address": endereco,
            "logradouro": logradouro,
            "numero": numero,
            "bairro": item.bairro,
            "cidade": item.cidade,
            "uf": item.uf,
            "cep": item.cep,
        })

    logger.info(f"[GEOCODE_BATCH] request recebido total={len(payload)}")

    uc = GeocodeAddressesUseCase()
    return uc.execute(payload, origem="api_batch")


@router.post(
    "/geocode/batch/jobs",
    dependencies=[Depends(verify_token)]
)
def geocode_batch_job(body: GeocodeBatchRequest):

    payload = []

    for idx, item in enumerate(body.addresses):
        payload.append({
            "id": idx,
            "logradouro": item.logradouro or item.endereco or item.address,
            "numero": item.numero or "",
            "bairro": item.bairro,
            "cidade": item.cidade,
            "uf": item.uf,
            "cep": item.cep,
            "address": item.address or item.endereco,
        })

    if not payload:
        raise HTTPException(status_code=400, detail="Nenhum endereço recebido")

    try:
        queue = fila_geocode()

        job = queue.enqueue(
            processar_batch_json,
            {
                "addresses": payload,
                "tenant_id": 1,
                "origem": "api_batch_json",
            },
            job_timeout=36000,
            meta={
                "progress": 0,
                "step": "Recebemos sua solicitacao",
                "origem": "api_batch_json",
            },
            description=f"geocode_batch_json:{uuid.uuid4()}"
        )

        logger.info(
            f"[GEOCODE_BATCH_JOB] job criado: {job.id} total={len(payload)}"
        )

        return {
            "status": "queued",
            "job_id": job.id,
            "total": len(payload)
        }

    except Exception as e:
        logger.error(f"[GEOCODE_BATCH_JOB][ERRO] {e}")
        raise HTTPException(status_code=500, detail=f"Erro ao criar job: {e}")


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
                "step": "Recebemos seu arquivo"
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


@router.get(
    "/job/{job_id}/batch-result",
    dependencies=[Depends(verify_token)]
)
def job_batch_result(job_id: str):

    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.get_status() != "finished":
        raise HTTPException(
            status_code=400,
            detail="Job ainda não finalizado"
        )

    result_key = job.meta.get("result_key") or f"geocode_batch_json_final:{job_id}"
    raw_result = redis_conn.get(result_key)

    if raw_result:
        if isinstance(raw_result, bytes):
            raw_result = raw_result.decode("utf-8")
        return JSONResponse(content=json.loads(raw_result))

    output_file = job.meta.get("output_file")

    if not output_file or not os.path.exists(output_file):
        raise HTTPException(
            status_code=404,
            detail="Arquivo de resultado não encontrado"
        )

    try:
        df_validos = pd.read_excel(output_file, sheet_name="geocodificados")
    except Exception:
        df_validos = pd.DataFrame()

    try:
        df_invalidos = pd.read_excel(output_file, sheet_name="invalidos")
    except Exception:
        df_invalidos = pd.DataFrame()

    results = []

    if not df_validos.empty:
        for _, row in df_validos.iterrows():
            results.append({
                "id": int(row["id"]) if pd.notna(row.get("id")) else None,
                "lat": row.get("lat") if pd.notna(row.get("lat")) else None,
                "lon": row.get("lon") if pd.notna(row.get("lon")) else None,
                "source": row.get("source") or "ok",
                "valid": True,
            })

    if not df_invalidos.empty:
        for _, row in df_invalidos.iterrows():
            results.append({
                "id": int(row["id"]) if pd.notna(row.get("id")) else None,
                "lat": None,
                "lon": None,
                "source": row.get("motivo_invalidacao") or "falha",
                "valid": False,
            })

    results = [r for r in results if r["id"] is not None]
    results.sort(key=lambda r: r["id"])

    return {
        "job_id": job.id,
        "status": "finished",
        "results": results,
        "stats": job.meta.get("result") or job.result or {}
    }

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

@router.get("/cache/search", dependencies=[Depends(verify_token)])
def buscar_cache(
    cidade: Optional[str] = None,
    uf: Optional[str] = None,
    endereco: Optional[str] = None,
    origem: Optional[str] = None,
    atualizado_de: Optional[str] = None,
    atualizado_ate: Optional[str] = None,
    limit: int = 50,
    offset: int = 0,
    order_by: str = "atualizado_em",
    order_dir: str = "desc",
):
    reader = DatabaseReader()

    results = reader.buscar_cache_filtrado(
        cidade=cidade,
        uf=uf,
        endereco=endereco,
        origem=origem,
        atualizado_de=atualizado_de,
        atualizado_ate=atualizado_ate,
        limit=limit,
        offset=offset,
        order_by=order_by,
        order_dir=order_dir,
    )

    return results


@router.post("/cache", dependencies=[Depends(verify_token)], status_code=201)
def criar_cache(payload: CacheCreateSchema):

    validar_lat_lon(payload.lat, payload.lon)

    reader = DatabaseReader()
    writer = DatabaseWriter(reader.conn)

    endereco_normalizado = build_cache_key(
        payload.logradouro,
        payload.numero or "",
        payload.cidade,
        payload.uf,
    )

    endereco_display = montar_endereco_display(
        payload.endereco,
        payload.logradouro,
        payload.numero,
        payload.cidade,
        payload.uf,
    )

    existente = reader.buscar_cache_por_chave_normalizada(endereco_normalizado)
    if existente:
        raise HTTPException(status_code=409, detail="Já existe um endereço cadastrado com essa chave de cache")

    existente_endereco = reader.buscar_cache_por_endereco(endereco_display)
    if existente_endereco:
        raise HTTPException(status_code=409, detail="Já existe um endereço cadastrado com esse endereço canônico")

    novo_id = writer.criar_cache_manual(
        endereco=endereco_display,
        endereco_normalizado=endereco_normalizado,
        lat=payload.lat,
        lon=payload.lon,
    )

    criado = reader.buscar_cache_por_id(novo_id)
    return jsonable_encoder(criado)


# ============================================================
# CACHE UPDATE
# ============================================================

@router.put("/cache/{id}", dependencies=[Depends(verify_token)])
def atualizar_cache(id: int, payload: CacheUpdateSchema):

    validar_lat_lon(payload.lat, payload.lon)

    reader = DatabaseReader()
    writer = DatabaseWriter(reader.conn)

    atual = reader.buscar_cache_por_id(id)
    if not atual:
        raise HTTPException(status_code=404, detail="Endereço não encontrado")

    if payload.logradouro and payload.cidade and payload.uf:
        endereco_normalizado = build_cache_key(
            payload.logradouro,
            payload.numero or "",
            payload.cidade,
            payload.uf,
        )
        endereco_display = montar_endereco_display(
            payload.endereco,
            payload.logradouro,
            payload.numero,
            payload.cidade,
            payload.uf,
        )

        existente = reader.buscar_cache_por_chave_normalizada(endereco_normalizado)
        if existente and existente["id"] != id:
            raise HTTPException(status_code=409, detail="Já existe outro endereço cadastrado com essa chave de cache")

        existente_endereco = reader.buscar_cache_por_endereco(endereco_display)
        if existente_endereco and existente_endereco["id"] != id:
            raise HTTPException(status_code=409, detail="Já existe outro endereço cadastrado com esse endereço canônico")

        writer.atualizar_cache(
            id,
            payload.lat,
            payload.lon,
            endereco=endereco_display,
            endereco_normalizado=endereco_normalizado,
        )
    else:
        writer.atualizar_cache(id, payload.lat, payload.lon)

    atualizado = reader.buscar_cache_por_id(id)
    return jsonable_encoder(atualizado)


@router.delete("/cache/{id}", dependencies=[Depends(verify_token)], status_code=204)
def excluir_cache(id: int):

    reader = DatabaseReader()
    writer = DatabaseWriter(reader.conn)

    atual = reader.buscar_cache_por_id(id)
    if not atual:
        raise HTTPException(status_code=404, detail="Endereço não encontrado")

    writer.excluir_cache(id)
    return None

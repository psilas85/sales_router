# sales_router/src/cadastros/api/pdvs_api.py

import os
import uuid
from datetime import date, datetime
from typing import List, Optional
from uuid import UUID

from fastapi import (
    APIRouter,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
    status,
)
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from rq.job import Job

from cadastros.application.pdv_use_case import CadastroPDVUseCase
from cadastros.entities.pdv_entity import CadastroPDV
from cadastros.infrastructure.pdv_repository import CnpjDuplicadoError
from cadastros.api.dependencies import verify_token
from cadastros.infrastructure.queue_factory import fila_import, redis_conn
from cadastros.jobs.import_clientes_job import processar_import_clientes

UPLOAD_DIR = "/app/data/cadastro_uploads"


router = APIRouter(
    prefix="/pdvs",
    tags=["PDVs (Clientes)"],
)

use_case = CadastroPDVUseCase()


# ============================================================
# SCHEMAS
# ============================================================

class PDVCreateSchema(BaseModel):
    cnpj: str = Field(..., min_length=11, max_length=20)
    razao_social: Optional[str] = Field(default=None, max_length=300)
    nome_fantasia: Optional[str] = Field(default=None, max_length=300)

    logradouro: str = Field(..., min_length=1, max_length=300)
    numero: str = Field(..., min_length=1, max_length=20)
    bairro: str = Field(..., min_length=1, max_length=200)
    cidade: str = Field(..., min_length=1, max_length=200)
    uf: str = Field(..., min_length=2, max_length=2)
    cep: str = Field(..., min_length=8, max_length=10)

    # Coordenadas opcionais (se vier, pula geocoding automático)
    pdv_lat: Optional[float] = None
    pdv_lon: Optional[float] = None

    pdv_vendas: Optional[float] = None

    janela_atendimento_inicio: Optional[int] = None
    janela_atendimento_fim: Optional[int] = None
    tempo_atendimento_min: Optional[float] = None
    is_estrategico: Optional[bool] = None

    ativo: bool = True


class PDVUpdateSchema(BaseModel):
    cnpj: Optional[str] = Field(default=None, max_length=20)
    razao_social: Optional[str] = Field(default=None, max_length=300)
    nome_fantasia: Optional[str] = Field(default=None, max_length=300)

    logradouro: Optional[str] = Field(default=None, max_length=300)
    numero: Optional[str] = Field(default=None, max_length=20)
    bairro: Optional[str] = Field(default=None, max_length=200)
    cidade: Optional[str] = Field(default=None, max_length=200)
    uf: Optional[str] = Field(default=None, min_length=2, max_length=2)
    cep: Optional[str] = Field(default=None, max_length=10)

    pdv_lat: Optional[float] = None
    pdv_lon: Optional[float] = None

    pdv_vendas: Optional[float] = None

    janela_atendimento_inicio: Optional[int] = None
    janela_atendimento_fim: Optional[int] = None
    tempo_atendimento_min: Optional[float] = None
    is_estrategico: Optional[bool] = None

    ativo: Optional[bool] = None


class PDVResponseSchema(BaseModel):
    id: UUID
    tenant_id: int
    ativo: bool

    cnpj: str
    razao_social: Optional[str]
    nome_fantasia: Optional[str]

    logradouro: str
    numero: str
    bairro: str
    cidade: str
    uf: str
    cep: str

    pdv_lat: Optional[float]
    pdv_lon: Optional[float]
    status_geolocalizacao: Optional[str]

    pdv_vendas: Optional[float]

    janela_atendimento_inicio: Optional[int]
    janela_atendimento_fim: Optional[int]
    tempo_atendimento_min: Optional[float]
    is_estrategico: Optional[bool]

    origem: str
    revisao_pendente: bool = False
    criado_em: Optional[datetime]
    atualizado_em: Optional[datetime]

    class Config:
        orm_mode = True


class PDVListResponseSchema(BaseModel):
    total: int
    limit: int
    offset: int
    items: List[PDVResponseSchema]


# ============================================================
# Utils
# ============================================================

def to_schema(obj: CadastroPDV) -> PDVResponseSchema:
    return PDVResponseSchema(**obj.__dict__)


# ============================================================
# CRIAR
# ============================================================

@router.post(
    "",
    response_model=PDVResponseSchema,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_token)],
)
def criar_pdv(
    payload: PDVCreateSchema,
    request: Request,
    geocode: bool = Query(True, description="Geocodificar automaticamente"),
):
    tenant_id = request.state.user["tenant_id"]

    pdv = CadastroPDV(
        id=None,
        tenant_id=tenant_id,
        ativo=payload.ativo,
        cnpj=payload.cnpj,
        razao_social=payload.razao_social,
        nome_fantasia=payload.nome_fantasia,
        logradouro=payload.logradouro,
        numero=payload.numero,
        bairro=payload.bairro,
        cidade=payload.cidade,
        uf=payload.uf,
        cep=payload.cep,
        pdv_lat=payload.pdv_lat,
        pdv_lon=payload.pdv_lon,
        status_geolocalizacao=None,
        pdv_vendas=payload.pdv_vendas,
        janela_atendimento_inicio=payload.janela_atendimento_inicio,
        janela_atendimento_fim=payload.janela_atendimento_fim,
        tempo_atendimento_min=payload.tempo_atendimento_min,
        is_estrategico=payload.is_estrategico,
        origem="manual",
    )

    try:
        criado = use_case.criar(pdv, geocode=geocode)
    except CnpjDuplicadoError as e:
        raise HTTPException(status.HTTP_409_CONFLICT, str(e))
    return to_schema(criado)


# ============================================================
# LISTAR (paginado + filtros)
# ============================================================

@router.get(
    "",
    response_model=PDVListResponseSchema,
    dependencies=[Depends(verify_token)],
)
def listar_pdvs(
    request: Request,
    ativo: Optional[bool] = Query(default=None, description="True=ativos, False=desativados, omitido=todos"),
    situacao: Optional[str] = Query(default=None, description="ativo | revisar | inativo (precede `ativo`)"),
    uf: Optional[str] = Query(default=None),
    ufs: Optional[str] = Query(default=None, description="UFs separadas por vírgula"),
    cidade: Optional[str] = Query(default=None),
    cidades: Optional[str] = Query(default=None, description="Cidades separadas por vírgula (match exato)"),
    busca: Optional[str] = Query(default=None, description="CNPJ, razão social ou nome fantasia"),
    is_estrategico: Optional[bool] = Query(default=None),
    com_coordenadas: Optional[bool] = Query(default=None),
    criado_de: Optional[date] = Query(default=None, description="Criado em a partir de (YYYY-MM-DD)"),
    criado_ate: Optional[date] = Query(default=None, description="Criado em até (YYYY-MM-DD)"),
    atualizado_de: Optional[date] = Query(default=None, description="Modificado em a partir de (YYYY-MM-DD)"),
    atualizado_ate: Optional[date] = Query(default=None, description="Modificado em até (YYYY-MM-DD)"),
    limit: int = Query(20, ge=1, le=3000),
    offset: int = Query(0, ge=0),
):
    tenant_id = request.state.user["tenant_id"]
    items, total = use_case.listar(
        tenant_id,
        ativo=ativo,
        situacao=situacao,
        uf=uf,
        ufs=(
            [u.strip() for u in ufs.split(",") if u.strip()] if ufs else None
        ),
        cidade=cidade,
        cidades=(
            [c.strip() for c in cidades.split(",") if c.strip()]
            if cidades
            else None
        ),
        busca=busca,
        is_estrategico=is_estrategico,
        com_coordenadas=com_coordenadas,
        criado_de=criado_de,
        criado_ate=criado_ate,
        atualizado_de=atualizado_de,
        atualizado_ate=atualizado_ate,
        limit=limit,
        offset=offset,
    )
    return PDVListResponseSchema(
        total=total,
        limit=limit,
        offset=offset,
        items=[to_schema(p) for p in items],
    )


# ============================================================
# BUSCAR POR ID
# ============================================================

@router.get(
    "/{pdv_id}",
    response_model=PDVResponseSchema,
    dependencies=[Depends(verify_token)],
)
def buscar_pdv(pdv_id: UUID, request: Request):
    tenant_id = request.state.user["tenant_id"]
    pdv = use_case.buscar_por_id(pdv_id, tenant_id)
    if not pdv:
        raise HTTPException(404, "PDV não encontrado.")
    return to_schema(pdv)


# ============================================================
# ATUALIZAR
# ============================================================

@router.put(
    "/{pdv_id}",
    response_model=PDVResponseSchema,
    dependencies=[Depends(verify_token)],
)
def atualizar_pdv(pdv_id: UUID, payload: PDVUpdateSchema, request: Request):
    tenant_id = request.state.user["tenant_id"]
    existente = use_case.buscar_por_id(pdv_id, tenant_id)
    if not existente:
        raise HTTPException(404, "PDV não encontrado.")

    # Merge: campos não enviados mantêm o valor anterior.
    pdv = CadastroPDV(
        id=pdv_id,
        tenant_id=tenant_id,
        ativo=payload.ativo if payload.ativo is not None else existente.ativo,
        cnpj=payload.cnpj or existente.cnpj,
        razao_social=payload.razao_social if payload.razao_social is not None else existente.razao_social,
        nome_fantasia=payload.nome_fantasia if payload.nome_fantasia is not None else existente.nome_fantasia,
        logradouro=payload.logradouro or existente.logradouro,
        numero=payload.numero or existente.numero,
        bairro=payload.bairro or existente.bairro,
        cidade=payload.cidade or existente.cidade,
        uf=payload.uf or existente.uf,
        cep=payload.cep or existente.cep,
        pdv_lat=payload.pdv_lat if payload.pdv_lat is not None else None,
        pdv_lon=payload.pdv_lon if payload.pdv_lon is not None else None,
        status_geolocalizacao=existente.status_geolocalizacao,
        pdv_vendas=payload.pdv_vendas if payload.pdv_vendas is not None else existente.pdv_vendas,
        janela_atendimento_inicio=(
            payload.janela_atendimento_inicio
            if payload.janela_atendimento_inicio is not None
            else existente.janela_atendimento_inicio
        ),
        janela_atendimento_fim=(
            payload.janela_atendimento_fim
            if payload.janela_atendimento_fim is not None
            else existente.janela_atendimento_fim
        ),
        tempo_atendimento_min=(
            payload.tempo_atendimento_min
            if payload.tempo_atendimento_min is not None
            else existente.tempo_atendimento_min
        ),
        is_estrategico=(
            payload.is_estrategico
            if payload.is_estrategico is not None
            else existente.is_estrategico
        ),
        origem=existente.origem,
        revisao_pendente=existente.revisao_pendente,
    )

    try:
        atualizado = use_case.atualizar(pdv, endereco_anterior=existente)
    except CnpjDuplicadoError as e:
        raise HTTPException(status.HTTP_409_CONFLICT, str(e))
    if not atualizado:
        raise HTTPException(404, "PDV não encontrado para atualização.")
    return to_schema(atualizado)


# ============================================================
# EXCLUIR (soft delete)
# ============================================================

@router.delete(
    "/{pdv_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(verify_token)],
)
def excluir_pdv(pdv_id: UUID, request: Request):
    tenant_id = request.state.user["tenant_id"]
    ok = use_case.excluir(pdv_id, tenant_id)
    if not ok:
        raise HTTPException(404, "PDV não encontrado ou já excluído.")
    return None


# ============================================================
# IMPORTAÇÃO EM LOTE (XLSX -> job assíncrono)
# ============================================================

@router.post("/importar", dependencies=[Depends(verify_token)])
async def importar_pdvs(
    request: Request,
    file: UploadFile = File(...),
    sobrescrever: bool = Query(
        default=False,
        description="Modo forçar: atualiza CNPJs já cadastrados em vez de pular",
    ),
):
    tenant_id = request.state.user["tenant_id"]

    nome = (file.filename or "").lower()
    if not nome.endswith((".xlsx", ".xls")):
        raise HTTPException(400, "Envie a planilha no formato .xlsx.")

    conteudo = await file.read()
    if len(conteudo) > 20 * 1024 * 1024:
        raise HTTPException(400, "Arquivo muito grande (máx 20MB).")

    os.makedirs(UPLOAD_DIR, exist_ok=True)
    file_path = os.path.join(UPLOAD_DIR, f"{uuid.uuid4().hex}.xlsx")
    with open(file_path, "wb") as f:
        f.write(conteudo)

    job = fila_import().enqueue(
        processar_import_clientes,
        {
            "file_path": file_path,
            "tenant_id": tenant_id,
            "filename": file.filename,
            "sobrescrever": sobrescrever,
        },
        job_timeout=3600,
        meta={"progress": 0, "step": "Na fila"},
    )
    return {"job_id": job.id}


@router.get("/importar/{job_id}", dependencies=[Depends(verify_token)])
def status_importacao(job_id: str):
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        raise HTTPException(404, "Importação não encontrada.")

    resp = {
        "job_id": job.id,
        "status": job.get_status(),
        "progress": job.meta.get("progress", 0),
        "step": job.meta.get("step", ""),
    }
    if job.is_finished:
        resp["resultado"] = job.result
    if job.is_failed:
        resp["erro"] = "Falha no processamento da importação."
    return resp


@router.get("/importar/{job_id}/resultado", dependencies=[Depends(verify_token)])
def download_resultado_importacao(job_id: str):
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        raise HTTPException(404, "Importação não encontrada.")

    if not job.is_finished or not isinstance(job.result, dict):
        raise HTTPException(400, "Importação ainda não finalizada.")

    arquivo = job.result.get("arquivo_resultado")
    if not arquivo or not os.path.exists(arquivo):
        raise HTTPException(404, "Esta importação não gerou relatório de linhas puladas.")

    return FileResponse(
        arquivo,
        filename=f"import_clientes_{job_id}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

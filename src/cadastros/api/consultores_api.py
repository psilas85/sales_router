# sales_router/src/cadastros/api/consultores_api.py

from datetime import datetime
from typing import List, Optional
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Request, status
from pydantic import BaseModel, Field, EmailStr
from psycopg2.errors import UniqueViolation

from cadastros.application.consultor_use_case import ConsultorUseCase
from cadastros.entities.consultor_entity import Consultor
from cadastros.api.dependencies import verify_token


router = APIRouter(
    prefix="/consultores",
    tags=["Consultores"]
)

use_case = ConsultorUseCase()


# ============================================================
# SCHEMAS
# ============================================================

class ConsultorCreateSchema(BaseModel):
    ativo: bool = False
    setor: Optional[str] = Field(default=None, max_length=10)
    consultor: str = Field(..., min_length=2, max_length=120)
    cpf: str = Field(..., regex=r"^\d{11}$")
    logradouro: str = Field(..., max_length=200)
    numero: str = Field(..., max_length=20)
    complemento: Optional[str] = Field(default=None, max_length=120)
    bairro: str = Field(..., max_length=120)
    cidade: str = Field(..., max_length=120)
    uf: str = Field(..., min_length=2, max_length=2)
    cep: str = Field(..., regex=r"^\d{8}$")
    celular: Optional[str] = Field(default=None, max_length=20)
    email: Optional[EmailStr] = None

    lat: float
    lon: float


class ConsultorUpdateSchema(BaseModel):
    ativo: Optional[bool] = None
    setor: Optional[str] = Field(default=None, max_length=10)
    consultor: Optional[str] = Field(default=None, min_length=2, max_length=120)
    logradouro: Optional[str] = Field(default=None, max_length=200)
    numero: Optional[str] = Field(default=None, max_length=20)
    complemento: Optional[str] = Field(default=None, max_length=120)
    bairro: Optional[str] = Field(default=None, max_length=120)
    cidade: Optional[str] = Field(default=None, max_length=120)
    uf: Optional[str] = Field(default=None, min_length=2, max_length=2)
    cep: Optional[str] = Field(default=None, max_length=10)
    celular: Optional[str] = Field(default=None, max_length=20)
    email: Optional[EmailStr] = None

    lat: float
    lon: float


class ConsultorResponseSchema(BaseModel):
    id: UUID
    tenant_id: int
    ativo: bool
    setor: Optional[str]
    consultor: str
    cpf: Optional[str]
    logradouro: Optional[str]
    numero: Optional[str]
    complemento: Optional[str]
    bairro: Optional[str]
    cidade: Optional[str]
    uf: Optional[str]
    cep: Optional[str]
    celular: Optional[str]
    email: Optional[str]

    lat: float
    lon: float

    criado_em: Optional[datetime]
    atualizado_em: Optional[datetime]

    class Config:
        orm_mode = True


# ============================================================
# UTILS
# ============================================================

def to_schema(obj: Consultor) -> ConsultorResponseSchema:
    return ConsultorResponseSchema(**obj.__dict__)


# ============================================================
# CRIAR
# ============================================================

@router.post(
    "",
    response_model=ConsultorResponseSchema,
    status_code=status.HTTP_201_CREATED,
    dependencies=[Depends(verify_token)]
)
def criar_consultor(payload: ConsultorCreateSchema, request: Request):

    tenant_id = request.state.user["tenant_id"]

    consultor = Consultor(
        id=None,
        tenant_id=tenant_id,
        ativo=payload.ativo,
        setor=payload.setor,
        consultor=payload.consultor,
        cpf=payload.cpf,
        logradouro=payload.logradouro,
        numero=payload.numero,
        complemento=payload.complemento,
        bairro=payload.bairro,
        cidade=payload.cidade,
        uf=payload.uf,
        cep=payload.cep,
        celular=payload.celular,
        email=payload.email,
        lat=payload.lat,
        lon=payload.lon,
    )

    try:
        criado = use_case.criar(consultor)
    except UniqueViolation as exc:
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Ja existe um consultor com este CPF neste tenant."
        ) from exc

    return to_schema(criado)


# ============================================================
# LISTAR
# ============================================================

@router.get(
    "",
    response_model=List[ConsultorResponseSchema],
    dependencies=[Depends(verify_token)]
)
def listar_consultores(request: Request):

    tenant_id = request.state.user["tenant_id"]

    dados = use_case.listar(tenant_id)

    return [to_schema(item) for item in dados]


# ============================================================
# BUSCAR POR ID
# ============================================================

@router.get(
    "/{consultor_id}",
    response_model=ConsultorResponseSchema,
    dependencies=[Depends(verify_token)]
)
def buscar_consultor(consultor_id: UUID, request: Request):

    tenant_id = request.state.user["tenant_id"]

    consultor = use_case.buscar_por_id(consultor_id, tenant_id)

    if not consultor:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Consultor não encontrado."
        )

    return to_schema(consultor)


# ============================================================
# ATUALIZAR
# ============================================================

@router.put(
    "/{consultor_id}",
    response_model=ConsultorResponseSchema,
    dependencies=[Depends(verify_token)]
)
def atualizar_consultor(
    consultor_id: UUID,
    payload: ConsultorUpdateSchema,
    request: Request
):

    tenant_id = request.state.user["tenant_id"]

    existente = use_case.buscar_por_id(consultor_id, tenant_id)

    if not existente:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Consultor não encontrado."
        )

    consultor = Consultor(
        id=consultor_id,
        tenant_id=tenant_id,
        ativo=payload.ativo if payload.ativo is not None else existente.ativo,
        setor=payload.setor or existente.setor,
        consultor=payload.consultor or existente.consultor,
        cpf=existente.cpf,
        logradouro=payload.logradouro or existente.logradouro,
        numero=payload.numero or existente.numero,
        complemento=payload.complemento or existente.complemento,
        bairro=payload.bairro or existente.bairro,
        cidade=payload.cidade or existente.cidade,
        uf=payload.uf or existente.uf,
        cep=payload.cep or existente.cep,
        celular=payload.celular or existente.celular,
        email=payload.email or existente.email,
        lat=payload.lat,
        lon=payload.lon,
    )

    atualizado = use_case.atualizar(consultor)

    if not atualizado:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Consultor não encontrado para atualização."
        )

    return to_schema(atualizado)


# ============================================================
# EXCLUIR
# ============================================================

@router.delete(
    "/{consultor_id}",
    status_code=status.HTTP_204_NO_CONTENT,
    dependencies=[Depends(verify_token)]
)
def excluir_consultor(consultor_id: UUID, request: Request):

    tenant_id = request.state.user["tenant_id"]

    excluido = use_case.excluir(consultor_id, tenant_id)

    if not excluido:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Consultor não encontrado."
        )

    return None
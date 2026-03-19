# sales_router/src/cadastros/entities/consultor_entity.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID


@dataclass
class Consultor:
    id: Optional[UUID]
    tenant_id: int
    setor: Optional[str]
    consultor: str
    cpf: str
    logradouro: str
    numero: str
    complemento: Optional[str]
    bairro: str
    cidade: str
    uf: str
    cep: str
    celular: Optional[str]
    email: Optional[str]

    # 🔥 OBRIGATÓRIOS
    lat: float
    lon: float

    criado_em: Optional[datetime] = None
    atualizado_em: Optional[datetime] = None
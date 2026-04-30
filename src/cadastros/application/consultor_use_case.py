# sales_router/src/cadastros/application/consultor_use_case.py

import unicodedata
from typing import List, Optional, Union
from uuid import UUID

from cadastros.entities.consultor_entity import Consultor
from cadastros.infrastructure.consultor_repository import ConsultorRepository


def _normalizar_cidade(valor: str | None) -> str | None:
    if not valor:
        return valor
    sem_acento = unicodedata.normalize("NFD", valor)
    sem_acento = "".join(c for c in sem_acento if unicodedata.category(c) != "Mn")
    return " ".join(sem_acento.upper().split())


def _normalizar_uf(valor: str | None) -> str | None:
    if not valor:
        return valor
    return valor.strip().upper()[:2]


class ConsultorUseCase:
    def __init__(self):
        self.repository = ConsultorRepository()

    def _normalizar(self, consultor: Consultor) -> Consultor:
        consultor.cidade = _normalizar_cidade(consultor.cidade)
        consultor.uf = _normalizar_uf(consultor.uf)
        return consultor

    def criar(self, consultor: Consultor) -> Consultor:
        if consultor.lat is None or consultor.lon is None:
            raise ValueError("lat e lon são obrigatórios")
        return self.repository.criar(self._normalizar(consultor))

    def listar(
        self,
        tenant_id: int,
        ativo: Optional[bool] = None,
        uf: Optional[str] = None,
        cidade: Optional[str] = None,
    ) -> List[Consultor]:
        return self.repository.listar(tenant_id, ativo=ativo, uf=uf, cidade=cidade)

    def buscar_por_id(self, consultor_id: UUID, tenant_id: int) -> Optional[Consultor]:
        return self.repository.buscar_por_id(consultor_id, tenant_id)

    def atualizar(self, consultor: Consultor) -> Optional[Consultor]:
        if consultor.lat is None or consultor.lon is None:
            raise ValueError("lat e lon são obrigatórios")
        return self.repository.atualizar(self._normalizar(consultor))

    def excluir(self, consultor_id: UUID, tenant_id: int) -> bool:
        return self.repository.excluir(consultor_id, tenant_id)
#sales_router/src/cadastros/application/consultor_use_case.py

from typing import List, Optional
from uuid import UUID

from cadastros.entities.consultor_entity import Consultor
from cadastros.infrastructure.consultor_repository import ConsultorRepository


class ConsultorUseCase:
    def __init__(self):
        self.repository = ConsultorRepository()

    def criar(self, consultor: Consultor) -> Consultor:
        return self.repository.criar(consultor)

    def listar(self, tenant_id: UUID) -> List[Consultor]:
        return self.repository.listar(tenant_id)

    def buscar_por_id(self, consultor_id: UUID, tenant_id: UUID) -> Optional[Consultor]:
        return self.repository.buscar_por_id(consultor_id, tenant_id)

    def atualizar(self, consultor: Consultor) -> Optional[Consultor]:
        return self.repository.atualizar(consultor)

    def excluir(self, consultor_id: UUID, tenant_id: UUID) -> bool:
        return self.repository.excluir(consultor_id, tenant_id)
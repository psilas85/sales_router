# sales_router/src/cadastros/application/consultor_use_case.py

from typing import List, Optional
from uuid import UUID

from cadastros.entities.consultor_entity import Consultor
from cadastros.infrastructure.consultor_repository import ConsultorRepository


class ConsultorUseCase:
    def __init__(self):
        self.repository = ConsultorRepository()

    def criar(self, consultor: Consultor) -> Consultor:

        # 🔥 validação obrigatória
        if consultor.lat is None or consultor.lon is None:
            raise ValueError("lat e lon são obrigatórios")

        return self.repository.criar(consultor)

    def listar(self, tenant_id: int) -> List[Consultor]:
        return self.repository.listar(tenant_id)

    def buscar_por_id(self, consultor_id: UUID, tenant_id: int) -> Optional[Consultor]:
        return self.repository.buscar_por_id(consultor_id, tenant_id)

    def atualizar(self, consultor: Consultor) -> Optional[Consultor]:

        if consultor.lat is None or consultor.lon is None:
            raise ValueError("lat e lon são obrigatórios")

        return self.repository.atualizar(consultor)

    def excluir(self, consultor_id: UUID, tenant_id: int) -> bool:
        return self.repository.excluir(consultor_id, tenant_id)
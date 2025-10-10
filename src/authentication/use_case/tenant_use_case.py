# sales_router/src/authentication/use_case/tenant_use_case.py

from authentication.entities.tenant import Tenant
from authentication.infrastructure.tenant_repository import TenantRepository

class TenantUseCase:
    def __init__(self):
        self.repo = TenantRepository()

    def setup_table(self):
        self.repo.create_table()

    def create_master_tenant(self):
        tenant = Tenant(
            razao_social="SalesRouter Tecnologia Ltda",
            nome_fantasia="SalesRouter",
            cnpj="00.000.000/0001-00",
            email_adm="admin@salesrouter.com",
            is_master=True
        )
        return self.repo.create(tenant)

    def create_tenant(self, razao_social, nome_fantasia, cnpj, email_adm):
        tenant = Tenant(
            razao_social=razao_social,
            nome_fantasia=nome_fantasia or razao_social,
            cnpj=cnpj,
            email_adm=email_adm,
            is_master=False
        )
        return self.repo.create(tenant)

    def list_tenants(self):
        return self.repo.list_all()

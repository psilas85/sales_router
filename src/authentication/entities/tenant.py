#sales_router/src/authentication/entities/tenant.py

from dataclasses import dataclass
from datetime import datetime

@dataclass
class Tenant:
    id: int = None
    razao_social: str = ""
    nome_fantasia: str = ""
    cnpj: str = ""
    email_adm: str = ""
    is_master: bool = False
    criado_em: datetime = datetime.now()

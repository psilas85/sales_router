#sales_router/src/authentication/entities/user.py

from dataclasses import dataclass
from datetime import datetime

@dataclass
class User:
    id: int = None
    tenant_id: int = None
    nome: str = ""
    email: str = ""
    senha_hash: str = ""
    role: str = "operacional"
    ativo: bool = True
    criado_em: datetime = datetime.now()


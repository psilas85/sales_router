# sales_router/src/authentication/entities/user.py

from dataclasses import dataclass
from typing import Optional, Literal

@dataclass
class User:
    id: Optional[int] = None
    tenant_id: int = 0
    nome: str = ""
    email: str = ""
    senha_hash: str = ""
    role: Literal["sales_router_adm", "tenant_adm", "tenant_operacional"] = "tenant_operacional"
    ativo: bool = True

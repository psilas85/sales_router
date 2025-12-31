#sales_router/src/authentication/api/schemas.py

# sales_router/src/authentication/api/schemas.py

from pydantic import BaseModel, EmailStr
from typing import Optional, Literal

UserRole = Literal[
    "sales_router_adm",
    "tenant_adm",
    "tenant_operacional"
]


class LoginSchema(BaseModel):
    email: EmailStr
    senha: str


class UserCreateSchema(BaseModel):
    nome: str
    email: EmailStr
    senha: str
    role: UserRole
    tenant_id: int


class UserUpdateSchema(BaseModel):
    nome: str
    email: EmailStr
    role: UserRole
    senha: Optional[str] = None


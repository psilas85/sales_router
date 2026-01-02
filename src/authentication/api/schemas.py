#sales_router/src/authentication/api/schemas.py

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
    tenant_id: Optional[int] = None


class UserUpdateSchema(BaseModel):
    nome: Optional[str] = None
    email: Optional[EmailStr] = None
    role: Optional[UserRole] = None
    senha: Optional[str] = None



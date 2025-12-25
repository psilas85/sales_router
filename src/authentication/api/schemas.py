#sales_router/src/authentication/api/schemas.py

from pydantic import BaseModel
from typing import Optional

class LoginSchema(BaseModel):
    email: str
    senha: str


class UserUpdateSchema(BaseModel):
    nome: str
    email: str
    role: str
    senha: Optional[str] = None

class UserCreateSchema(BaseModel):
    nome: str
    email: str
    senha: str
    role: str
    tenant_id: int

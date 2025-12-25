#sales_router/src/authentication/api/routes.py

from fastapi import APIRouter, HTTPException, Request
from authentication.use_case.tenant_use_case import TenantUseCase
from authentication.use_case.user_use_case import UserUseCase
from authentication.domain.auth_service import AuthService, role_required
import jwt
import os
from datetime import datetime

# =====================================================
# 🔐 Configurações JWT
# =====================================================
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "salesrouter-secret-key")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")

router = APIRouter()

tenant_use_case = TenantUseCase()
user_use_case = UserUseCase()
auth_service = AuthService()

# =====================================================
# 📦 TENANTS
# =====================================================

@router.post("/tenants", tags=["Tenants"])
@role_required(["sales_router_adm"])
def create_tenant(
    request: Request,
    razao_social: str,
    nome_fantasia: str,
    cnpj: str,
    email_adm: str,
):
    try:
        tenant = tenant_use_case.create_tenant(
            razao_social, nome_fantasia, cnpj, email_adm
        )
        return {"message": "Tenant criado com sucesso", "tenant": tenant.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/tenants", tags=["Tenants"])
@role_required(["sales_router_adm"])
def list_tenants(request: Request):
    tenants = tenant_use_case.list_tenants()
    return {"tenants": [t.__dict__ for t in tenants]}


# =====================================================
# 👤 USUÁRIOS
# =====================================================

@router.post("/users", tags=["Usuários"])
@role_required(["sales_router_adm", "tenant_adm"])
def create_user(
    request: Request,
    nome: str,
    email: str,
    senha: str,
    role: str,
    tenant_id: int,
):
    creator_role = request.state.user["role"]

    if creator_role == "sales_router_adm" and role == "tenant_adm":
        user = user_use_case.create_tenant_admin(tenant_id, nome, email, senha)
    elif creator_role == "tenant_adm" and role == "tenant_operacional":
        user = user_use_case.create_tenant_operacional(tenant_id, nome, email, senha)
    else:
        raise HTTPException(status_code=403, detail="Permissão insuficiente.")

    return {"message": "Usuário criado com sucesso", "user": user.__dict__}


@router.get("/users", tags=["Usuários"])
@role_required(["sales_router_adm", "tenant_adm"])
def list_users(request: Request):
    role = request.state.user["role"]
    tenant_id = request.state.user["tenant_id"]

    if role == "sales_router_adm":
        users = user_use_case.list_users()
    else:
        users = user_use_case.list_users_by_tenant(tenant_id)

    return {"users": [u.__dict__ for u in users]}


@router.put("/users/{user_id}/deactivate", tags=["Usuários"])
@role_required(["tenant_adm", "sales_router_adm"])
def deactivate_user(request: Request, user_id: int):
    try:
        user = user_use_case.deactivate_user(user_id)
        return {"message": f"Usuário {user.nome} inativado"}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# =====================================================
# 🔐 AUTENTICAÇÃO
# =====================================================

from fastapi import Query

@router.post("/login", tags=["Autenticação"])
def login(
    email: str = Query(...),
    senha: str = Query(...)
):
    token = user_use_case.login(email, senha)
    if not token:
        raise HTTPException(status_code=401, detail="Credenciais inválidas")
    return {"token": token}


@router.get("/me", tags=["Autenticação"])
@role_required(["sales_router_adm", "tenant_adm", "tenant_operacional"])
def get_me(request: Request):
    return {"user": request.state.user}


@router.post("/verify-token", tags=["Autenticação"])
def verify_token(payload: dict):
    token = payload.get("token")
    if not token:
        raise HTTPException(status_code=400, detail="Token não informado")

    try:
        decoded = jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])

        exp = decoded.get("exp")
        if exp and datetime.utcnow().timestamp() > exp:
            raise HTTPException(status_code=401, detail="Token expirado")

        return {
            "user_id": decoded.get("user_id"),
            "tenant_id": decoded.get("tenant_id"),
            "role": decoded.get("role"),
            "email": decoded.get("email"),
        }

    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expirado")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token inválido")

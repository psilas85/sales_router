# sales_router/src/authentication/api/routes.py

from fastapi import APIRouter, HTTPException, Request
from authentication.use_case.tenant_use_case import TenantUseCase
from authentication.use_case.user_use_case import UserUseCase
from authentication.domain.auth_service import AuthService, role_required

router = APIRouter()

tenant_use_case = TenantUseCase()
user_use_case = UserUseCase()
auth_service = AuthService()

# =====================================================
# 📦 TENANTS
# =====================================================

@router.post("/tenants", tags=["Tenants"])
@role_required(["sales_router_adm"])
def create_tenant(request: Request, razao_social: str, nome_fantasia: str, cnpj: str, email_adm: str):
    """Cria um novo tenant (empresa). Apenas sales_router_adm pode criar."""
    try:
        tenant = tenant_use_case.create_tenant(razao_social, nome_fantasia, cnpj, email_adm)
        return {"message": "✅ Tenant criado com sucesso!", "tenant": tenant.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/tenants", tags=["Tenants"])
@role_required(["sales_router_adm"])
def list_tenants(request: Request):
    """Lista todos os tenants cadastrados. Restrito ao SalesRouter."""
    tenants = tenant_use_case.list_tenants()
    return {"tenants": [t.__dict__ for t in tenants]}


# =====================================================
# 👤 USUÁRIOS
# =====================================================

@router.post("/users", tags=["Usuários"])
@role_required(["sales_router_adm", "tenant_adm"])
def create_user(request: Request, nome: str, email: str, senha: str, role: str, tenant_id: int):
    """
    Cria um novo usuário conforme permissões do criador:
    - sales_router_adm → pode criar tenant_adm
    - tenant_adm → pode criar tenant_operacional
    """
    creator_role = request.state.user["role"]

    if creator_role == "sales_router_adm" and role == "tenant_adm":
        user = user_use_case.create_tenant_admin(tenant_id, nome, email, senha)
    elif creator_role == "tenant_adm" and role == "tenant_operacional":
        user = user_use_case.create_tenant_operacional(tenant_id, nome, email, senha)
    else:
        raise HTTPException(status_code=403, detail="Permissão insuficiente para criar este tipo de usuário.")

    return {"message": "✅ Usuário criado com sucesso!", "user": user.__dict__}


@router.get("/users", tags=["Usuários"])
@role_required(["sales_router_adm", "tenant_adm"])
def list_users(request: Request):
    """Lista usuários conforme escopo do criador."""
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
    """Inativa um usuário (não deleta)."""
    try:
        user = user_use_case.deactivate_user(user_id)
        return {"message": f"Usuário {user.nome} inativado com sucesso."}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


# =====================================================
# 🔐 LOGIN / AUTENTICAÇÃO
# =====================================================

@router.post("/login", tags=["Autenticação"])
def login(email: str, senha: str):
    """Autentica o usuário e retorna token JWT."""
    token = user_use_case.login(email, senha)
    if not token:
        raise HTTPException(status_code=401, detail="Credenciais inválidas.")
    return {"token": token}


@router.get("/auth/me", tags=["Autenticação"])
@role_required(["sales_router_adm", "tenant_adm", "tenant_operacional"])
def get_me(request: Request):
    """Retorna as informações do usuário autenticado."""
    return {"user": request.state.user}

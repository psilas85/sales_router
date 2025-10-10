# sales_router/src/authentication/api/routes.py

from fastapi import APIRouter, HTTPException
from authentication.use_case.tenant_use_case import TenantUseCase
from authentication.use_case.user_use_case import UserUseCase
from authentication.domain.auth_service import AuthService

router = APIRouter()

tenant_use_case = TenantUseCase()
user_use_case = UserUseCase()
auth_service = AuthService()

# =====================================================
# 📦 TENANTS
# =====================================================

@router.post("/tenants")
def create_tenant(razao_social: str, nome_fantasia: str, cnpj: str, email_adm: str):
    """Cria um novo tenant (empresa)."""
    try:
        tenant = tenant_use_case.create_tenant(razao_social, nome_fantasia, cnpj, email_adm)
        return {"message": "Tenant criado com sucesso!", "tenant": tenant.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/tenants")
def list_tenants():
    """Lista todos os tenants cadastrados."""
    tenants = tenant_use_case.list_tenants()
    return {"tenants": [t.__dict__ for t in tenants]}


# =====================================================
# 👤 USUÁRIOS
# =====================================================

@router.post("/users")
def create_user(nome: str, email: str, senha: str, role: str, tenant_id: int):
    """Cria um novo usuário vinculado a um tenant."""
    try:
        user = user_use_case.create_user(nome, email, senha, role, tenant_id)
        return {"message": "Usuário criado com sucesso!", "user": user.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/users")
def list_users():
    """Lista todos os usuários cadastrados."""
    users = user_use_case.list_users()
    return {"users": [u.__dict__ for u in users]}


# =====================================================
# 🔐 LOGIN / AUTENTICAÇÃO
# =====================================================

@router.post("/login")
def login(email: str, senha: str):
    """Autentica o usuário e retorna token JWT."""
    try:
        user = auth_service.user_repo.find_by_email(email)
        if not user:
            raise HTTPException(status_code=404, detail="Usuário não encontrado.")
        if not auth_service.verify_password(senha, user.senha):
            raise HTTPException(status_code=401, detail="Senha incorreta.")
        token = auth_service.generate_token(user, user.email)
        return {"token": token, "user": user.__dict__}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

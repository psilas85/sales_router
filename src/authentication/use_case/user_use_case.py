# sales_router/src/authentication/use_case/user_use_case.py

import os
from authentication.domain.auth_service import AuthService
from authentication.infrastructure.user_repository import UserRepository
from authentication.entities.user import User

class UserUseCase:
    def __init__(self):
        self.repo = UserRepository()
        self.auth = AuthService()

    def setup_table(self):
        self.repo.create_table()

    # =====================================================
    # CRIAÇÕES
    # =====================================================

    def create_sales_router_admin(self, tenant_id):
        """
        Cria o administrador master do SalesRouter.
        Usa senha vinda de variável de ambiente ADMIN_PASSWORD para maior segurança.
        """
        admin_password = os.getenv("ADMIN_PASSWORD", "Psilas@85")  # fallback para dev
        senha_hash = self.auth.hash_password(admin_password)

        user = User(
            tenant_id=tenant_id,
            nome="Paulo Silas",
            email="paulo.silas@igotech.com.br",
            senha_hash=senha_hash,
            role="sales_router_adm",
            ativo=True
        )
        return self.repo.create(user)

    def create_tenant_admin(self, tenant_id, nome, email, senha):
        senha_hash = self.auth.hash_password(senha)
        user = User(
            tenant_id=tenant_id,
            nome=nome,
            email=email,
            senha_hash=senha_hash,
            role="tenant_adm",
            ativo=True
        )
        return self.repo.create(user)

    def create_tenant_operacional(self, tenant_id, nome, email, senha):
        senha_hash = self.auth.hash_password(senha)
        user = User(
            tenant_id=tenant_id,
            nome=nome,
            email=email,
            senha_hash=senha_hash,
            role="tenant_operacional",
            ativo=True
        )
        return self.repo.create(user)

    # =====================================================
    # LOGIN
    # =====================================================

    def login(self, email, senha):
        user = self.repo.find_by_email(email)
        if not user or not self.auth.verify_password(senha, user.senha_hash):
            return None
        return self.auth.generate_token(user.id, user.tenant_id, user.role)


    # =====================================================
    # LISTAGEM / ADMIN
    # =====================================================

    def list_users(self):
        return self.repo.list_all()

    def list_users_by_tenant(self, tenant_id):
        return self.repo.list_by_tenant(tenant_id)

    def deactivate_user(self, user_id):
        return self.repo.deactivate(user_id)

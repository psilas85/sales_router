# sales_router/src/authentication/use_case/user_use_case.py

import os
from authentication.domain.auth_service import AuthService
from authentication.infrastructure.user_repository import UserRepository
from authentication.entities.user import User


class UserUseCase:
    def __init__(self):
        self.repo = UserRepository()
        self.auth = AuthService()

    # =====================================================
    # CRIAÇÕES
    # =====================================================

    def create_sales_router_admin(self, tenant_id):
        admin_password = os.getenv("ADMIN_PASSWORD", "Psilas@85")
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
        return self.repo.create(User(
            tenant_id=tenant_id,
            nome=nome,
            email=email,
            senha_hash=self.auth.hash_password(senha),
            role="tenant_adm",
            ativo=True
        ))

    def create_tenant_operacional(self, tenant_id, nome, email, senha):
        return self.repo.create(User(
            tenant_id=tenant_id,
            nome=nome,
            email=email,
            senha_hash=self.auth.hash_password(senha),
            role="tenant_operacional",
            ativo=True
        ))

    # =====================================================
    # LOGIN
    # =====================================================

    def login(self, email, senha):
        user = self.repo.find_by_email(email)

        if not user:
            return None

        # 🔒 BLOQUEIA USUÁRIO INATIVO
        if not user.ativo:
            raise Exception("Usuário inativo")

        if not self.auth.verify_password(senha, user.senha_hash):
            return None

        return self.auth.generate_token(
            user.id,
            user.tenant_id,
            user.role,
            user.email
        )


    # =====================================================
    # LISTAGEM
    # =====================================================

    def list_users(self):
        return self.repo.list_all()

    def list_users_by_tenant(self, tenant_id):
        return self.repo.list_by_tenant(tenant_id)

    # =====================================================
    # ATIVA / DESATIVA
    # =====================================================

    def deactivate_user(self, user_id, requester):
        user = self.repo.find_by_id(user_id)
        if not user:
            raise Exception("Usuário não encontrado")

        # 🔒 MASTER NÃO PODE SER DESATIVADO
        if user.role == "sales_router_adm":
            raise Exception("Usuário master não pode ser desativado")

        # 🔒 NÃO PODE SE AUTO-DESATIVAR
        if requester["user_id"] == user.id:
            raise Exception("Você não pode desativar a si mesmo")

        # 🔒 TENANT ADM SÓ NO PRÓPRIO TENANT
        if (
            requester["role"] == "tenant_adm"
            and user.tenant_id != requester["tenant_id"]
        ):
            raise Exception("Permissão insuficiente")

        user.ativo = False
        return self.repo.update_partial(user)

    def activate_user(self, user_id, requester):
        user = self.repo.find_by_id(user_id)
        if not user:
            raise Exception("Usuário não encontrado")

        # 🔒 TENANT ADM SÓ NO PRÓPRIO TENANT
        if (
            requester["role"] == "tenant_adm"
            and user.tenant_id != requester["tenant_id"]
        ):
            raise Exception("Permissão insuficiente")

        user.ativo = True
        return self.repo.update_partial(user)

    # =====================================================
    # EDIÇÃO SEGURA
    # =====================================================

    def update_user(self, user_id, nome, email, role, senha, requester):
        user = self.repo.find_by_id(user_id)
        if not user:
            raise Exception("Usuário não encontrado")

        if user.role == "sales_router_adm":
            raise Exception("Usuário master não pode ser alterado")

        if (
            requester["role"] == "tenant_adm"
            and user.tenant_id != requester["tenant_id"]
        ):
            raise Exception("Permissão insuficiente")

        if requester["user_id"] == user.id:
            if email and email != user.email:
                raise Exception("Você não pode alterar seu próprio email")
            if role and role != user.role:
                raise Exception("Você não pode alterar seu próprio perfil")

        if nome:
            user.nome = nome
        if email:
            user.email = email
        if role:
            user.role = role

        if senha:
            if len(senha.strip()) < 8:
                raise Exception("Senha deve ter no mínimo 8 caracteres")
            user.senha_hash = self.auth.hash_password(senha)

        return self.repo.update_partial(user)



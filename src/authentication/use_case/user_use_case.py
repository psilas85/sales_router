#sales_router/src/authentication/use_case/user_use_case.py

from authentication.entities.user import User
from authentication.infrastructure.user_repository import UserRepository
from authentication.domain.auth_service import AuthService

class UserUseCase:
    def __init__(self):
        self.repo = UserRepository()
        self.auth = AuthService()

    def setup_table(self):
        self.repo.create_table()

    def create_admin_user(self, tenant_id):
        senha_hash = self.auth.hash_password("admin123")
        user = User(
            tenant_id=tenant_id,
            nome="Administrador Master",
            email="admin@salesrouter.com",
            senha_hash=senha_hash,
            role="admin",
            ativo=True
        )
        return self.repo.create(user)

    def login(self, email, senha):
        row = self.repo.find_by_email(email)
        if not row:
            return None
        id, tenant_id, nome, email, senha_hash, role, ativo = row
        if not self.auth.verify_password(senha, senha_hash):
            return None
        token = self.auth.generate_token(id, tenant_id, role)
        return token

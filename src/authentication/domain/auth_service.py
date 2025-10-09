#sales_router/src/authentication/domain/auth_service.py

import bcrypt
import jwt
import os
from datetime import datetime, timedelta

SECRET_KEY = os.getenv("JWT_SECRET_KEY", "salesrouter-secret-key")
ALGORITHM = "HS256"

class AuthService:
    def hash_password(self, senha):
        return bcrypt.hashpw(senha.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')

    def verify_password(self, senha, senha_hash):
        return bcrypt.checkpw(senha.encode('utf-8'), senha_hash.encode('utf-8'))

    def generate_token(self, user_id, tenant_id, role):
        payload = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "role": role,
            "exp": datetime.utcnow() + timedelta(hours=8)
        }
        return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

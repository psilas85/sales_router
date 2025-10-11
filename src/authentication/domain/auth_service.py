# src/authentication/domain/auth_service.py

import bcrypt
import jwt
import os
from datetime import datetime, timedelta
from functools import wraps
from fastapi import HTTPException, Request, status

# ==============================
# 🔐 Configurações JWT centralizadas
# ==============================
JWT_SECRET_KEY = os.getenv("JWT_SECRET_KEY", "salesrouter-secret-key")
JWT_ALGORITHM = os.getenv("JWT_ALGORITHM", "HS256")
JWT_EXP_HOURS = int(os.getenv("JWT_EXP_HOURS", 8))


class AuthService:
    def hash_password(self, senha):
        return bcrypt.hashpw(senha.encode("utf-8"), bcrypt.gensalt()).decode("utf-8")

    def verify_password(self, senha, senha_hash):
        return bcrypt.checkpw(senha.encode("utf-8"), senha_hash.encode("utf-8"))

    def generate_token(self, user_id, tenant_id, role):
        payload = {
            "user_id": user_id,
            "tenant_id": tenant_id,
            "role": role,
            "exp": datetime.utcnow() + timedelta(hours=JWT_EXP_HOURS),
        }
        return jwt.encode(payload, JWT_SECRET_KEY, algorithm=JWT_ALGORITHM)

    def decode_token(self, token: str):
        try:
            return jwt.decode(token, JWT_SECRET_KEY, algorithms=[JWT_ALGORITHM])
        except jwt.ExpiredSignatureError:
            raise HTTPException(status_code=401, detail="Token expirado")
        except jwt.InvalidTokenError:
            raise HTTPException(status_code=401, detail="Token inválido")


def role_required(roles: list[str]):
    """Decorator para proteger rotas FastAPI com base no role do usuário."""
    def decorator(func):
        @wraps(func)
        async def wrapper(request: Request, *args, **kwargs):
            auth_header = request.headers.get("Authorization")
            if not auth_header or not auth_header.startswith("Bearer "):
                raise HTTPException(status_code=401, detail="Token não fornecido")

            token = auth_header.split(" ")[1]
            auth = AuthService()

            try:
                payload = auth.decode_token(token)
            except HTTPException as e:
                raise e

            if payload.get("role") not in roles:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail="Acesso negado: privilégio insuficiente"
                )

            request.state.user = {
                "user_id": payload.get("user_id"),
                "tenant_id": payload.get("tenant_id"),
                "role": payload.get("role"),
            }

            import inspect
            if inspect.iscoroutinefunction(func):
                return await func(request, *args, **kwargs)
            else:
                return func(request, *args, **kwargs)

        return wrapper
    return decorator

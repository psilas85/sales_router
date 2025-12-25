#sales_router/src/pdv_preprocessing/api/dependencies.py

# pdv_preprocessing/api/dependencies.py

import os
import jwt
from fastapi import Request, HTTPException, status

# =====================================================
# 🔐 Configurações JWT (OBRIGATÓRIO vir do .env)
# =====================================================
try:
    JWT_SECRET_KEY = os.environ["JWT_SECRET_KEY"]
except KeyError:
    raise RuntimeError("JWT_SECRET_KEY não definido no ambiente")

JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")


# =====================================================
# 🔐 Dependency de autenticação
# =====================================================
async def verify_token(request: Request):
    """
    Valida JWT localmente.
    Injeta usuário autenticado em request.state.user
    """

    auth_header = request.headers.get("Authorization")

    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token ausente ou inválido."
        )

    token = auth_header.replace("Bearer ", "").strip()

    try:
        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM]
        )

        # Campos mínimos obrigatórios
        required_fields = ["user_id", "tenant_id", "role", "email"]
        for field in required_fields:
            if field not in payload:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"Token inválido: campo '{field}' ausente."
                )

        # Injetar usuário no request
        request.state.user = {
            "user_id": payload["user_id"],
            "tenant_id": payload["tenant_id"],
            "role": payload["role"],
            "email": payload["email"],
        }

    except jwt.ExpiredSignatureError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token expirado."
        )

    except jwt.InvalidTokenError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido."
        )

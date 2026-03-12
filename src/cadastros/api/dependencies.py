#sales_router/src/cadastros/api/dependencies.py

import os
import jwt
from fastapi import Request, HTTPException, status


try:
    JWT_SECRET_KEY = os.environ["JWT_SECRET_KEY"]
except KeyError:
    raise RuntimeError("JWT_SECRET_KEY não definido no ambiente")

JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")


async def verify_token(request: Request):

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

        required_fields = ["user_id", "tenant_id", "role", "email"]

        for field in required_fields:
            if field not in payload:
                raise HTTPException(
                    status_code=status.HTTP_401_UNAUTHORIZED,
                    detail=f"Token inválido: campo '{field}' ausente."
                )

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
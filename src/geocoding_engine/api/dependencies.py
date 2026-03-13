#Sales_router/src/geocoding_engine/api/dependencies.py

import os
import jwt

from fastapi import Request, HTTPException, status, Security
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials


JWT_SECRET_KEY = os.environ["JWT_SECRET_KEY"]
JWT_ALGORITHM = os.environ.get("JWT_ALGORITHM", "HS256")

security = HTTPBearer()


async def verify_token(
    request: Request,
    credentials: HTTPAuthorizationCredentials = Security(security)
):

    token = credentials.credentials

    try:

        payload = jwt.decode(
            token,
            JWT_SECRET_KEY,
            algorithms=[JWT_ALGORITHM]
        )

        request.state.user = payload

    except jwt.InvalidTokenError:

        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token inválido"
        )
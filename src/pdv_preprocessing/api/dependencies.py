#sales_router/src/pdv_preprocessing/api/dependencies.py

import os
import requests
from fastapi import Request, HTTPException, status

AUTH_SERVICE_URL = os.getenv("AUTH_SERVICE_URL", "http://authentication_service:8000")

async def verify_token(request: Request):
    """
    Middleware para validar o token JWT emitido pelo módulo de autenticação central.
    Adiciona o payload decodificado em request.state.user.
    """
    auth_header = request.headers.get("Authorization")
    if not auth_header or not auth_header.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Token ausente ou inválido."
        )

    token = auth_header.split(" ")[1]

    try:
        response = requests.post(
            f"{AUTH_SERVICE_URL}/auth/verify-token",
            json={"token": token},
            timeout=5
        )

        if response.status_code != 200:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token inválido ou expirado."
            )

        request.state.user = response.json()

    except requests.RequestException:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Serviço de autenticação indisponível."
        )

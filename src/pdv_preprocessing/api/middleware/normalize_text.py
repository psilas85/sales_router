#sales_router/src/pdv_preprocessing/api/middleware/normalize_text.py

import unicodedata
from fastapi import Request
from starlette.middleware.base import BaseHTTPMiddleware

# --------------------------------------------------
# 🔧 Função canônica de normalização
# --------------------------------------------------
def normalize_text(value: str):
    return (
        unicodedata.normalize("NFD", value)
        .encode("ascii", "ignore")
        .decode("utf-8")
        .upper()
        .strip()
    )

# --------------------------------------------------
# 🧩 Middleware com WHITELIST de campos
# --------------------------------------------------
class NormalizeTextMiddleware(BaseHTTPMiddleware):

    # Campos que PODEM ser normalizados
    ALLOWED_FIELDS = {
        "cidade",
        "bairro",
        "logradouro",
        "uf",
        "descricao",
    }

    async def dispatch(self, request: Request, call_next):

        # -------------------------
        # QUERY PARAMS
        # -------------------------
        if request.query_params:
            qp = dict(request.query_params)
            for k, v in qp.items():
                if k in self.ALLOWED_FIELDS and isinstance(v, str):
                    qp[k] = normalize_text(v)

            request._query_params = qp  # override interno

        # -------------------------
        # BODY JSON (PUT / POST / PATCH)
        # -------------------------
        if request.method in {"POST", "PUT", "PATCH"}:
            try:
                body = await request.json()
                if isinstance(body, dict):
                    for k, v in body.items():
                        if k in self.ALLOWED_FIELDS and isinstance(v, str):
                            body[k] = normalize_text(v)
                    request._body = body
            except Exception:
                pass  # body não é JSON → ignora

        return await call_next(request)

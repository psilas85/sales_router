#sales_router/src/pdv_preprocessing/api/pdv_preprocessing_api.py

# ==========================================================
# 📦 src/pdv_preprocessing/api/pdv_preprocessing_api.py
# ==========================================================

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from .routes import router as pdv_router

import numpy as np
import json
import uvicorn
import unicodedata

# ==========================================================
# 🔧 Função canônica de normalização de texto
# ==========================================================
def normalize_text(value: str):
    return (
        unicodedata.normalize("NFD", value)
        .encode("ascii", "ignore")
        .decode("utf-8")
        .upper()
        .strip()
    )

# ==========================================================
# 🧩 FastAPI App
# ==========================================================
app = FastAPI(
    title="SalesRouter PDV Preprocessing API",
    description="Serviço de pré-processamento e gestão de PDVs (multi-tenant)",
    version="1.2.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
    servers=[{"url": "/pdv", "description": "PDV Preprocessing service behind API Gateway"}],
)

# ==========================================================
# 🧩 Middleware — NORMALIZA TEXTO (WHITELIST)
# ==========================================================
@app.middleware("http")
async def normalize_text_middleware(request: Request, call_next):

    ALLOWED_FIELDS = {"cidade", "bairro", "logradouro", "uf", "descricao"}

    # -------------------------
    # Query Params
    # -------------------------
    if request.query_params:
        qp = dict(request.query_params)
        for k, v in qp.items():
            if k in ALLOWED_FIELDS and isinstance(v, str):
                qp[k] = normalize_text(v)

        request._query_params = qp  # override interno

    # -------------------------
    # Body JSON (PUT / POST / PATCH)
    # -------------------------
    if request.method in {"POST", "PUT", "PATCH"}:
        try:
            body = await request.json()
            if isinstance(body, dict):
                for k, v in body.items():
                    if k in ALLOWED_FIELDS and isinstance(v, str):
                        body[k] = normalize_text(v)
                request._body = body
        except Exception:
            pass  # body não é JSON

    return await call_next(request)

# ==========================================================
# 🧹 Middleware global de saneamento JSON (JÁ EXISTENTE)
# ==========================================================
@app.middleware("http")
async def sanitize_json_response(request: Request, call_next):
    response = await call_next(request)

    if "application/json" in response.headers.get("content-type", ""):
        try:
            raw_body = b"".join([chunk async for chunk in response.body_iterator])
            content = json.loads(raw_body)

            def clean(obj):
                if isinstance(obj, dict):
                    return {k: clean(v) for k, v in obj.items()}
                elif isinstance(obj, list):
                    return [clean(i) for i in obj]
                elif isinstance(obj, float):
                    if np.isnan(obj) or np.isinf(obj):
                        return None
                    return obj
                else:
                    return obj

            cleaned = clean(content)
            return JSONResponse(content=cleaned, status_code=response.status_code)
        except Exception:
            return response

    return response

# ==========================================================
# 🔀 Rotas principais
# ==========================================================
app.include_router(pdv_router, prefix="/pdv")

# ==========================================================
# 🩺 Health check
# ==========================================================
@app.get("/", tags=["Status"])
def root():
    return {"status": "SalesRouter PDV Preprocessing API online 🚀"}

# ==========================================================
# 🚀 Execução standalone (dev)
# ==========================================================
if __name__ == "__main__":
    uvicorn.run(
        "pdv_preprocessing.api.pdv_preprocessing_api:app",
        host="0.0.0.0",
        port=8000,
    )

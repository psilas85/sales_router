#sales_router/src/sales_clusterization/api/cluster_api.py

# ============================================================
# 📦 src/sales_clusterization/api/cluster_api.py
# ============================================================

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sales_clusterization.api.routes import router as cluster_router
from sales_clusterization.api.operacional_routes import (
    router as operacional_router,
)
import numpy as np
import json
import uvicorn

# ============================================================
# 🚀 App
# ============================================================

app = FastAPI(
    title="SalesRouter Clusterization API",
    description="Serviço de clusterização de PDVs (multi-tenant)",
    version="1.0.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
)

# ============================================================
# 🌍 CORS
# ============================================================

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ============================================================
# 🧹 Middleware — sanitizar JSON (NaN / Infinity)
# ============================================================

@app.middleware("http")
async def sanitize_json_response(request: Request, call_next):
    response = await call_next(request)

    content_type = response.headers.get("content-type", "")
    if "application/json" not in content_type:
        return response

    try:
        raw_body = b"".join([chunk async for chunk in response.body_iterator])
        content = json.loads(raw_body)

        def clean(obj):
            if isinstance(obj, dict):
                return {k: clean(v) for k, v in obj.items()}
            if isinstance(obj, list):
                return [clean(i) for i in obj]
            if isinstance(obj, float):
                if np.isnan(obj) or np.isinf(obj):
                    return None
                return obj
            return obj

        cleaned = clean(content)
        return JSONResponse(content=cleaned, status_code=response.status_code)

    except Exception:
        return response

# ============================================================
# 🔀 ROTAS — PADRÃO CORRETO
# ============================================================
# 🔥 ESTE É O PONTO-CHAVE
# Tudo agora vive oficialmente em /cluster/*
# nginx NÃO precisa ser alterado
# frontend NÃO precisa ser alterado
# ============================================================

app.include_router(
    cluster_router,
    prefix="/cluster",
    tags=["Clusterização"]
)

# Setorização da Execução Operacional — endpoints sob /cluster/operacional/*.
# Aditivo: não afeta as rotas da Simulação.
app.include_router(
    operacional_router,
    prefix="/cluster",
    tags=["Operacional"]
)

# ============================================================
# 🩺 Health local
# ============================================================

@app.get("/")
def root():
    return {"status": "SalesRouter Clusterization API online 🚀"}

# ============================================================
# 🚀 Execução standalone (dev)
# ============================================================

if __name__ == "__main__":
    uvicorn.run(
        "sales_clusterization.api.cluster_api:app",
        host="0.0.0.0",
        port=8004,
        reload=True
    )

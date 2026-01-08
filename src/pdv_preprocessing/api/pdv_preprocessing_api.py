#sales_router/src/pdv_preprocessing/api/pdv_preprocessing_api.py

# ==========================================================
# 📦 src/pdv_preprocessing/api/pdv_preprocessing_api.py
# ==========================================================

from fastapi import FastAPI
from .routes import router as pdv_router

import uvicorn

# ==========================================================
# 🧩 FastAPI App
# ==========================================================
app = FastAPI(
    title="SalesRouter PDV Preprocessing API",
    description="Serviço de pré-processamento e gestão de PDVs (multi-tenant)",
    version="1.2.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
    servers=[
        {
            "url": "/pdv",
            "description": "PDV Preprocessing service behind API Gateway"
        }
    ],
)

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

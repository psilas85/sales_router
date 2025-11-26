#sales_router/src/sales_clusterization/api/cluster_api.py

# ============================================================
# 📦 src/sales_clusterization/api/cluster_api.py
# ============================================================

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from .routes import router as cluster_router
import numpy as np
import json
import uvicorn

app = FastAPI(
    title="SalesRouter Clusterization API",
    description="Serviço de clusterização de PDVs (multi-tenant)",
    version="1.0.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
    servers=[{"url": "/cluster", "description": "Cluster service behind API Gateway"}],
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
# 🧹 Middleware para limpar JSON inválido
# ============================================================
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
                return obj

            cleaned = clean(content)
            return JSONResponse(content=cleaned, status_code=response.status_code)
        except Exception:
            return response

    return response


# ============================================================
# 🔀 Rotas principais
# ============================================================
app.include_router(cluster_router, prefix="/cluster")


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
    uvicorn.run("sales_clusterization.api.cluster_api:app", host="0.0.0.0", port=8004)

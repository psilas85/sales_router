#sales_router/src/authentication/api/authentication_api.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from authentication.api.routes import router as auth_router

# ==========================================================
# 🧩 Configuração principal da aplicação FastAPI
# ==========================================================
app = FastAPI(
    title="SalesRouter Authentication API",
    description="Módulo de autenticação multi-tenant do SalesRouter",
    version="1.1.0",

    # ✅ Corrigido: mantém os caminhos padrão internos
    openapi_url="/openapi.json",
    docs_url="/docs",

    servers=[{"url": "/auth", "description": "Auth service behind API Gateway"}],
)

# ==========================================================
# 🌍 Middleware CORS
# ==========================================================
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================================
# 🔀 Rotas da API
# ==========================================================
app.include_router(auth_router)  # sem prefixo

# ==========================================================
# 🩺 Health check / Root endpoint
# ==========================================================
@app.get("/", tags=["Status"])
def root():
    """Verifica se a API está online."""
    return {"status": "SalesRouter Authentication API online 🚀"}

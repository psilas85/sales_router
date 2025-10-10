# sales_router/src/authentication/api/authentication_api.py

from fastapi import FastAPI
from authentication.api.routes import router

app = FastAPI(
    title="SalesRouter Authentication API",
    description="Módulo de autenticação e multi-tenant do SalesRouter",
    version="1.0.0"
)

# Inclui rotas
app.include_router(router)

@app.get("/", tags=["Status"])
def root():
    return {"status": "SalesRouter Authentication API online 🚀"}

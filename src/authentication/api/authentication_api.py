#sales_router/src/authentication/api/authentication_api.py

from fastapi import FastAPI
from authentication.api.routes import router as auth_router

app = FastAPI(
    title="SalesRouter Authentication API",
    description="Módulo de autenticação multi-tenant do SalesRouter",
    version="1.1.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
)

app.include_router(auth_router, prefix="/auth")

@app.get("/", tags=["Status"])
def root():
    return {"status": "SalesRouter Authentication API online 🚀"}

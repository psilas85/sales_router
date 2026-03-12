# sales_router/src/cadastros/api/cadastros_api.py

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from cadastros.api.consultores_api import router as consultores_router


app = FastAPI(
    title="SalesRouter Cadastros API",
    description="Serviço de cadastros do SalesRouter",
    version="1.0.0",
    openapi_url="/openapi.json",
    docs_url="/docs",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(consultores_router)


@app.get("/")
def root():
    return {"status": "SalesRouter Cadastros API online 🚀"}
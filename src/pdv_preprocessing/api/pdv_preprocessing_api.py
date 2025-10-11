# src/pdv_preprocessing/api/pdv_preprocessing_api.py
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from .routes import router as pdv_router
import uvicorn

app = FastAPI(
    title="SalesRouter PDV Preprocessing API",
    description="Serviço interno de pré-processamento e gestão de PDVs",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Rotas principais
app.include_router(pdv_router, prefix="/pdv", tags=["PDV Preprocessing"])

@app.get("/")
def root():
    return {"status": "SalesRouter PDV Preprocessing API online 🚀"}

if __name__ == "__main__":
    uvicorn.run("api.pdv_preprocessing_api:app", host="0.0.0.0", port=8000)

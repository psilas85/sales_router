from fastapi import FastAPI
from clusterization_engine.api.routes import router as cluster_router

app = FastAPI(title="Clusterization Engine API")
app.include_router(cluster_router)

@app.get("/health")
def health():
    return {"status": "ok", "service": "clusterization_engine"}

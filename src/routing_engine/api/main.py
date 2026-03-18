#sales_router/src/routing_engine/api/main.py

from fastapi import FastAPI
from routing_engine.api.routes import router

app = FastAPI(
    title="Routing Engine",
    servers=[
        {"url": "/routing-engine"}
    ]
)

app.include_router(router, prefix="/api/v1")
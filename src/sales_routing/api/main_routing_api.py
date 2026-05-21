#sales_router/src/sales_routing/api/main_routing_api.py

from fastapi import FastAPI
from sales_routing.api.routes import router
from sales_routing.api.operacional_routes import router as operacional_router

app = FastAPI(
    title="Sales Routing API",
    version="1.0.0",
    description="API de roteirização do SalesRouter"
)

app.include_router(router, prefix="/routing")
# Roteirização da Execução Operacional — /routing/operacional/*
app.include_router(operacional_router, prefix="/routing")

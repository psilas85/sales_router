#sales_router/src/sales_routing/api/main_routing_api.py

from fastapi import FastAPI
from sales_routing.api.routes import router

app = FastAPI(
    title="Sales Routing API",
    version="1.0.0",
    description="API de roteirização do SalesRouter"
)

app.include_router(router, prefix="/routing")

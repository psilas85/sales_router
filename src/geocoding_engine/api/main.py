#sales_router/src/geocoding_engine/api/main.py

from fastapi import FastAPI
from geocoding_engine.api.routes import router
from loguru import logger
import sys
import os


LOG_DIR = "/app/logs"
os.makedirs(LOG_DIR, exist_ok=True)

logger.remove()
logger.add(sys.stdout, level="INFO")
logger.add(f"{LOG_DIR}/geocoding_engine.log", rotation="50 MB", retention="10 days")


app = FastAPI(
    title="Geocoding Engine API",
    version="1.0.0",
    description="API de geocoding do SalesRouter",
    servers=[
        {"url": "/geocode"}
    ]
)

app.include_router(
    router,
    prefix="/api/v1"
)
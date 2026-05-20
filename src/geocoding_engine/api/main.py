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


@app.on_event("startup")
def _preload_municipio_polygons():
    """Aquece o cache dos polígonos IBGE no boot. O _load_polygons() lê o
    municipios.geojson (~500 MB) e constrói as geometrias shapely — ~30 s na
    primeira chamada. Sem isso, esse custo cai na primeira geocodificação do
    usuário; aqui ele passa para o startup do container."""
    import time
    from geocoding_engine.domain.municipio_polygon_validator import _load_polygons

    try:
        t0 = time.time()
        polygons = _load_polygons()
        logger.info(
            f"[STARTUP] polígonos IBGE pré-carregados: {len(polygons)} "
            f"municípios em {time.time() - t0:.1f}s"
        )
    except Exception as e:
        logger.warning(f"[STARTUP] falha ao pré-carregar polígonos IBGE: {e}")
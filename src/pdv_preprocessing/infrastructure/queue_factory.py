#sales_router/src/pdv_preprocessing/infrastructure/queue_factory.py

from rq import Queue
from redis import Redis
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
redis_conn = Redis.from_url(REDIS_URL)

def fila_nominatim():
    return Queue("mkp_nominatim", connection=redis_conn, default_timeout=120)

def fila_google():
    return Queue("mkp_google", connection=redis_conn, default_timeout=120)

def fila_viacep():
    return Queue("mkp_viacep", connection=redis_conn, default_timeout=120)

def fila_resultados():
    return Queue("mkp_resultados", connection=redis_conn, default_timeout=120)

# Nova fila opcional (dispatcher)
def fila_geocode():
    return Queue("mkp_geocode", connection=redis_conn, default_timeout=300)

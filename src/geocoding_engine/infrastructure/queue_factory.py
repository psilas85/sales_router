#sales_router/src/geocoding_engine/infrastructure/queue_factory.py

from rq import Queue
from redis import Redis
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

redis_conn = Redis.from_url(REDIS_URL)

def fila_geocode():
    return Queue(
        "geocode_jobs",
        connection=redis_conn,
        default_timeout=36000
    )
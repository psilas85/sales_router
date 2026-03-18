#sales_router/src/routing_engine/infrastructure/queue_factory.py

import os
from rq import Queue
from redis import Redis

def get_queue(name: str = "routing_jobs"):

    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    conn = Redis.from_url(redis_url)

    return Queue(name, connection=conn)
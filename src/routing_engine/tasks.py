#sales_router/src/routing_engine/tasks.py

from routing_engine.infrastructure.queue_factory import get_queue
from routing_engine.workers.routing_jobs import processar_routing

def enqueue_routing(file_path: str):

    queue = get_queue("routing_batch")

    job = queue.enqueue(
        processar_routing,
        file_path,
        job_timeout=3600
    )

    return job.id
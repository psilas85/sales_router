#sales_router/src/geocoding_engine/tasks.py

from redis import Redis
from rq import Queue
from rq.retry import Retry
import argparse
from loguru import logger

from geocoding_engine.workers.geocode_jobs import processar_geocode


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("arquivo")
    parser.add_argument("--tenant_id", default=1, type=int)
    parser.add_argument("--origem", default="manual_cli")

    args = parser.parse_args()

    conn = Redis(host="redis", port=6379)

    q = Queue("geocode_jobs", connection=conn)

    job = q.enqueue(
        processar_geocode,
        args.arquivo,
        job_timeout=36000,
        retry=Retry(max=2, interval=[10, 30]),
        meta={
            "tenant_id": args.tenant_id,
            "origem": args.origem,
            "progress": 0,
            "step": "Recebemos sua solicitacao"
        },
        description=f"geocode:{args.arquivo}"
    )

    logger.info(f"🚀 Job criado: {job.id}")

    print(f"Job criado: {job.id}")


if __name__ == "__main__":
    main()
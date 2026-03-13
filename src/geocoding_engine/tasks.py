#sales_router/src/geocoding_engine/tasks.py

from redis import Redis
from rq import Queue
import argparse

from geocoding_engine.workers.geocode_jobs import processar_geocode


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("arquivo")

    args = parser.parse_args()

    conn = Redis(host="redis", port=6379)

    q = Queue("geocode_jobs", connection=conn)

    job = q.enqueue(
        processar_geocode,
        args.arquivo,
        job_timeout=36000
    )

    print(f"Job criado: {job.id}")


if __name__ == "__main__":
    main()
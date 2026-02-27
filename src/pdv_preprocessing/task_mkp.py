# ============================================================
# 📦 sales_router/src/pdv_preprocessing/task_mkp.py
# ============================================================

import logging
from redis import Redis
from rq import Queue

from pdv_preprocessing.jobs.job_master_mkp import job_master_mkp

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)


def main():
    import argparse

    parser = argparse.ArgumentParser(
        description="Enfileira job_master_mkp (MKP MASTER)"
    )
    parser.add_argument(
        "--tenant",
        type=int,
        required=True,
        help="Tenant ID"
    )
    parser.add_argument(
        "--input_id",
        type=str,
        required=True,
        help="Input ID gerado no pré-processamento MKP"
    )
    parser.add_argument(
        "--descricao",
        type=str,
        required=True,
        help="Descrição do job"
    )

    args = parser.parse_args()

    tenant_id = args.tenant
    input_id = args.input_id
    descricao = args.descricao.strip()[:60]

    redis_conn = Redis(host="redis", port=6379)
    queue = Queue("mkp_master", connection=redis_conn)

    job = queue.enqueue(
        job_master_mkp,
        tenant_id,
        input_id,
        descricao,
        job_timeout=36000,
    )

    logging.info(
        "🚀 Job MKP MASTER enfileirado | "
        f"job_id={job.id} | tenant={tenant_id} | input_id={input_id}"
    )


if __name__ == "__main__":
    main()

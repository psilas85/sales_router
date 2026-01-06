#sales_router/src/pdv_preprocessing/tasks.py

# ============================================================
# 📦 src/pdv_preprocessing/tasks.py  (VERSÃO LIMPA)
# ============================================================

import sys
import logging
from redis import Redis
from rq import Queue

from pdv_preprocessing.pdv_jobs import processar_pdv

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def main():
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("tenant_id")
    parser.add_argument("arquivo")
    parser.add_argument("descricao")
    parser.add_argument("--usar_google", action="store_true")

    args = parser.parse_args()

    tenant_id = int(args.tenant_id)
    file_path = args.arquivo
    descricao = args.descricao

    conn = Redis(host="redis", port=6379)
    q = Queue("pdv_jobs", connection=conn)

    meta = {"usar_google": args.usar_google}

    job = q.enqueue(
        processar_pdv,
        tenant_id,
        file_path,
        descricao,
        meta=meta,
        job_timeout=36000
    )



    logging.info(
        f"🚀 Job enfileirado: {job.id} | tenant={tenant_id} | google={args.usar_google}"
    )



if __name__ == "__main__":
    main()

# ============================================================
# 📦 src/pdv_preprocessing/tasks.py
# ============================================================

import sys
import logging
from redis import Redis
from rq import Queue
from pdv_preprocessing.jobs import processar_csv

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

def main():
    if len(sys.argv) < 4:
        print("Uso: python -m src.pdv_preprocessing.tasks <tenant_id> <arquivo> <descricao>")
        sys.exit(1)

    tenant_id = int(sys.argv[1])
    file_path = sys.argv[2]
    descricao = sys.argv[3]

    conn = Redis(host="redis", port=6379)
    q = Queue("pdv_jobs", connection=conn)

    job = q.enqueue(processar_csv, tenant_id, file_path, descricao)
    logging.info(f"🚀 Job enfileirado: {job.id} → tenant={tenant_id}, arquivo={file_path}")

if __name__ == "__main__":
    main()

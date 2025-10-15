#sales_router/src/sales_clusterization/cli/run_cluster_job.py

import argparse
import logging
import uuid
from redis import Redis
from rq import Queue
from datetime import datetime
from src.sales_clusterization.jobs import processar_clusterizacao
from src.database.pipeline_history_service import registrar_historico_pipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")


def main():
    parser = argparse.ArgumentParser(
        description="Enfileira job assíncrono de clusterização (SalesRouter multi-tenant)"
    )

    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID (ex: 1)")
    parser.add_argument("--uf", required=True, help="UF dos PDVs (ex: CE, SP, RJ)")
    parser.add_argument("--cidade", required=True, help="Cidade dos PDVs (ex: Fortaleza)")
    parser.add_argument("--algo", default="kmeans", choices=["kmeans", "dbscan"], help="Algoritmo de clusterização")
    parser.add_argument("--k", type=int, default=None, help="K forçado (opcional)")
    parser.add_argument("--modo_forcar", action="store_true", help="Força reprocessamento e limpeza anterior")
    parser.add_argument("--fila", default="pipeline_jobs", help="Nome da fila Redis (default: pipeline_jobs)")

    args = parser.parse_args()

    # ============================================================
    # 🧠 Configuração inicial
    # ============================================================
    tenant_id = args.tenant
    uf = args.uf.upper()
    cidade = args.cidade
    algo = args.algo
    k = args.k
    modo_forcar = args.modo_forcar
    fila = args.fila

    redis_conn = Redis(host="redis", port=6379)
    queue = Queue(fila, connection=redis_conn)

    # Cria identificador único de job
    job_id = f"cluster-{tenant_id}-{datetime.utcnow().strftime('%Y%m%d%H%M%S')}-{uuid.uuid4().hex[:6]}"

    logging.info(f"🚀 Enfileirando job de clusterização | tenant={tenant_id} | {uf}-{cidade} | algo={algo}")

    # ============================================================
    # 🧾 Registro inicial do histórico
    # ============================================================
    registrar_historico_pipeline(
        tenant_id, job_id, "clusterization",
        status="queued", mensagem=f"Clusterização enfileirada ({uf}-{cidade})"
    )

    # ============================================================
    # 🚀 Enfileira o job no Redis
    # ============================================================
    job = queue.enqueue(
        processar_clusterizacao,
        job_id, tenant_id, uf, cidade, algo, k, modo_forcar,
        job_timeout=7200,
    )

    logging.info(f"✅ Job enfileirado com sucesso!")
    logging.info(f"🧠 Job ID Redis: {job.id}")
    logging.info(f"📊 Job ID lógico: {job_id}")
    logging.info(f"🕓 Fila: {fila}")

    print("\n=== 🚀 JOB DE CLUSTERIZAÇÃO ENFILEIRADO ===")
    print(f"Tenant ID: {tenant_id}")
    print(f"Localidade: {uf} - {cidade}")
    print(f"Algoritmo: {algo}")
    print(f"K forçado: {k}")
    print(f"Modo forçar: {modo_forcar}")
    print(f"Job ID lógico: {job_id}")
    print(f"Job ID Redis: {job.id}")
    print(f"Fila: {fila}")
    print("===========================================")


if __name__ == "__main__":
    main()

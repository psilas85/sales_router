# sales_router/src/database/pipeline_history_service.py

import time
from datetime import datetime
import json
from loguru import logger
from src.database.db_connection import get_connection


# ============================================================
# 🧠 Função principal: registrar histórico do pipeline
# ============================================================
def registrar_historico_pipeline(
    tenant_id: int,
    job_id: str,
    etapa: str,
    status: str,
    mensagem: str,
    metadata: dict | None = None,
):
    """
    Registra histórico de execução das etapas do pipeline assíncrono.
    Utiliza a tabela historico_pipeline_jobs.
    """
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            INSERT INTO historico_pipeline_jobs
                (tenant_id, job_id, etapa, status, mensagem, metadata, criado_em)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                job_id,
                etapa,
                status,
                mensagem,
                json.dumps(metadata or {}, ensure_ascii=False),
                datetime.utcnow(),
            ),
        )
        conn.commit()
        cur.close()
        conn.close()

        logger.info(
            f"💾 Histórico salvo: tenant={tenant_id} | etapa={etapa} | status={status}"
        )

    except Exception as e:
        logger.error(f"❌ Erro ao registrar histórico do pipeline: {e}", exc_info=True)


# ============================================================
# ⚙️ Context Manager para etapas do pipeline
# ============================================================
class EtapaPipeline:
    """
    Context manager para registrar início/fim automático de cada etapa do pipeline.

    Uso:
        with EtapaPipeline(tenant_id, job_id, 'routing', arquivo='data/input.csv'):
            # executar código da etapa
    """
    def __init__(self, tenant_id: int, job_id: str, etapa: str, arquivo: str = None):
        self.tenant_id = tenant_id
        self.job_id = job_id
        self.etapa = etapa
        self.arquivo = arquivo
        self.inicio = None

    def __enter__(self):
        self.inicio = time.time()
        metadata = {"arquivo_referencia": self.arquivo} if self.arquivo else {}
        registrar_historico_pipeline(
            tenant_id=self.tenant_id,
            job_id=self.job_id,
            etapa=self.etapa,
            status="running",
            mensagem=f"Iniciando etapa {self.etapa}",
            metadata=metadata,
        )
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        duracao = round(time.time() - self.inicio, 2) if self.inicio else None
        status = "done" if exc_type is None else "error"
        mensagem = (
            "Etapa concluída com sucesso"
            if exc_type is None
            else f"Erro: {exc_val}"
        )

        metadata = {
            "arquivo_referencia": self.arquivo,
            "duracao_segundos": duracao,
        }

        registrar_historico_pipeline(
            tenant_id=self.tenant_id,
            job_id=self.job_id,
            etapa=self.etapa,
            status=status,
            mensagem=mensagem,
            metadata=metadata,
        )

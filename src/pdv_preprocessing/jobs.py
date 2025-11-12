# ============================================================
# 📦 src/pdv_preprocessing/jobs.py (versão aprimorada)
# ============================================================

import logging
import subprocess
import json
from datetime import datetime
from rq import get_current_job
from database.db_connection import get_connection_context

logger = logging.getLogger(__name__)

# ============================================================
# 🧾 Função auxiliar: salvar histórico com segurança
# ============================================================
def salvar_historico(
    tenant_id,
    input_id,
    descricao,
    status,
    arquivo,
    total,
    validos,
    invalidos,
    arquivo_invalidos,
    mensagem,
):
    """Grava o histórico do processamento vinculado ao input_id."""
    try:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO historico_pdv_jobs
                    (tenant_id, input_id, descricao, arquivo, status, total_processados, validos,
                     invalidos, arquivo_invalidos, mensagem, criado_em)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        tenant_id,
                        input_id,
                        descricao,
                        arquivo,
                        status,
                        total,
                        validos,
                        invalidos,
                        arquivo_invalidos,
                        mensagem,
                        datetime.utcnow(),
                    ),
                )
        logger.info(f"💾 Histórico gravado (input_id={input_id})")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar histórico no banco: {e}", exc_info=True)


# ============================================================
# 🚀 Função principal: processar CSV via subprocesso
# ============================================================
def processar_csv(tenant_id, file_path, descricao):
    """Executa o pré-processamento de PDVs chamando o script principal em subprocesso."""
    job = get_current_job()
    job_id = job.id if job else "local"
    logger.info(f"🚀 Iniciando job {job_id} para tenant {tenant_id}")

    try:
        comando = [
            "python",
            "-u",
            "src/pdv_preprocessing/main_pdv_preprocessing.py",
            "--tenant",
            str(tenant_id),
            "--arquivo",
            file_path,
            "--descricao",
            descricao,
        ]

        if job:
            job.meta.update({"step": "Iniciando processamento", "progress": 0})
            job.save_meta()

        logger.info(f"▶️ Executando comando: {' '.join(comando)}")

        process = subprocess.Popen(
            comando,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )

        try:
            stdout, _ = process.communicate(timeout=3600)
        except subprocess.TimeoutExpired:
            process.kill()
            raise TimeoutError("⏱️ Tempo limite excedido (1h) no pré-processamento de PDVs")

        if process.returncode != 0:
            raise RuntimeError(f"❌ Processo retornou código {process.returncode}:\n{stdout}")

        resumo = {}
        for line in stdout.splitlines():
            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and "status" in obj:
                    resumo = obj
                    break
            except json.JSONDecodeError:
                continue

        input_id = resumo.get("input_id")
        status = resumo.get("status", "done")
        validos = resumo.get("validos", 0)
        invalidos = resumo.get("invalidos", 0)
        total = resumo.get("total_processados", validos + invalidos)
        arquivo_invalidos = resumo.get("arquivo_invalidos")
        arquivo_nome = resumo.get("arquivo")

        logger.info(f"✅ Job {job_id} concluído: {validos} válidos / {invalidos} inválidos")

        if job:
            job.meta.update({"step": "Finalizado", "progress": 100})
            job.save_meta()

        return {
            "status": status,
            "job_id": job_id,
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao,
            "arquivo": arquivo_nome,
            "total_processados": total,
            "validos": validos,
            "invalidos": invalidos,
            "arquivo_invalidos": arquivo_invalidos,
        }

    except Exception as e:
        logger.error(f"💥 Erro no job {job_id}: {e}", exc_info=True)
        if job:
            job.meta.update({"step": "Erro", "progress": 100})
            job.save_meta()

        # fallback de histórico
        salvar_historico(
            tenant_id,
            resumo.get("input_id") if "resumo" in locals() else None,
            descricao,
            "error",
            file_path.split("/")[-1],
            0,
            0,
            0,
            None,
            str(e),
        )

        return {
            "status": "error",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "descricao": descricao,
            "error": str(e),
        }

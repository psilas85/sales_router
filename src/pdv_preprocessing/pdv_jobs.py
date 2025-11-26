#sales_router/src/pdv_preprocessing/pdv_jobs.py

# ============================================================
# 📦 src/pdv_preprocessing/pdv_jobs.py — versão FINAL
# ============================================================

import logging
import subprocess
import json
from datetime import datetime
from uuid import uuid4, UUID
from rq import get_current_job

from database.db_connection import get_connection_context
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter

logger = logging.getLogger(__name__)

import sys

logger = logging.getLogger("pdv_jobs")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)

logger.handlers = [handler]

# ============================================================
# 🧾 Salvar histórico fallback (quando main quebra)
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
    try:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO historico_pdv_jobs
                    (tenant_id, input_id, descricao, arquivo, status,
                     total_processados, validos, invalidos, arquivo_invalidos,
                     mensagem, criado_em)
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
        logger.info(f"💾 Histórico fallback gravado (input_id={input_id})")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar histórico fallback: {e}", exc_info=True)


# ============================================================
# 🚀 Função principal do worker
# ============================================================
def processar_csv(tenant_id, file_path, descricao):
    job = get_current_job()

    # ------------------------------------------------------------
    # 🔥 Garantir que SEMPRE temos um UUID válido para job_id
    # ------------------------------------------------------------
    if job:
        try:
            job_id = str(UUID(job.id))   # garante formato UUID
        except:
            job_id = str(uuid4())        # fallback
    else:
        job_id = str(uuid4())            # CLI também vira UUID

    writer = DatabaseWriter()

    logger.info(f"🚀 Iniciando job {job_id} para tenant {tenant_id}")

    try:
        comando = [
            "python3",
            "-u",
            "src/pdv_preprocessing/main_pdv_preprocessing.py",
            "--tenant", str(tenant_id),
            "--arquivo", file_path,
            "--descricao", descricao,
        ]

        # 🔥 Se o job vier com meta "usar_google", adiciona flag
        try:
            usar_google_job = job.meta.get("usar_google", False) if job else False
        except:
            usar_google_job = False

        if usar_google_job:
            comando.append("--usar_google")


        if job:
            job.meta.update({"step": "Iniciando", "progress": 0})
            job.save_meta()

        logger.info(f"▶️ Executando: {' '.join(comando)}")

        # ------------------------------------------------------------
        # 🔥 Streaming em tempo real
        # ------------------------------------------------------------
        proc = subprocess.Popen(
            comando,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        resumo = {}
        json_line = None

        for line in proc.stdout:
            line = line.rstrip()
            logger.info(f"[MAIN] {line}")

            try:
                obj = json.loads(line)
                if isinstance(obj, dict) and "status" in obj:
                    resumo = obj
                    json_line = line
            except json.JSONDecodeError:
                pass

        proc.wait()

        if proc.returncode != 0:
            raise RuntimeError(f"main_pdv_preprocessing retornou código {proc.returncode}")

        if not resumo and json_line:
            try:
                resumo = json.loads(json_line)
            except:
                pass

        if not resumo:
            raise RuntimeError("Não foi possível capturar o JSON final do main_pdv_preprocessing")

        input_id = resumo.get("input_id")
        status = resumo.get("status")
        validos = resumo.get("validos", 0)
        invalidos = resumo.get("invalidos", 0)
        total = resumo.get("total_processados", validos + invalidos)
        arquivo_invalidos = resumo.get("arquivo_invalidos")
        arquivo_nome = resumo.get("arquivo")

        logger.info(f"✅ Job {job_id}: {validos} válidos / {invalidos} inválidos")

        if job:
            job.meta.update({"step": "Finalizado", "progress": 100})
            job.save_meta()

        # ------------------------------------------------------------
        # 💾 grava no histórico principal
        # ------------------------------------------------------------
        writer.salvar_historico_pdv_job(
            tenant_id=tenant_id,
            arquivo=arquivo_nome,
            status=status,
            total_processados=total,
            validos=validos,
            invalidos=invalidos,
            arquivo_invalidos=arquivo_invalidos,
            mensagem="OK",
            inseridos=validos,
            descricao=descricao,
            input_id=input_id,
            job_id=job_id,
            sobrescritos=0,
        )

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

        salvar_historico(
            tenant_id,
            resumo.get("input_id") if "resumo" in locals() else None,
            descricao,
            "error",
            file_path.split("/")[-1],
            0, 0, 0,
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

#sales_router/src/pdv_preprocessing/jobs.py

import logging
import subprocess
import json
from datetime import datetime
from rq import get_current_job
from database.db_connection import get_connection  # ✅ usa o core global

logger = logging.getLogger(__name__)


# ============================================================
# 🧾 Função auxiliar: salvar histórico no banco
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
        conn = get_connection()
        cur = conn.cursor()
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
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"💾 Histórico gravado (input_id={input_id})")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar histórico no banco: {e}", exc_info=True)


# ============================================================
# 🚀 Função principal: processar CSV via subprocesso
# ============================================================
def processar_csv(tenant_id, file_path, descricao):
    """
    Executa o pré-processamento de PDVs chamando o script principal em subprocesso.
    Gera automaticamente um input_id (UUID) e grava o histórico no banco.
    """
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

        job.meta["step"] = "Iniciando processamento"
        job.meta["progress"] = 0
        job.save_meta()

        logger.info(f"▶️ Executando comando: {' '.join(comando)}")
        job.meta["step"] = "Executando pipeline"
        job.meta["progress"] = 25
        job.save_meta()

        process = subprocess.Popen(
            comando,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
        )

        stdout_lines = []
        for line in iter(process.stdout.readline, ""):
            line = line.strip()
            if line:
                logger.info(f"[{job_id}] {line}")
                stdout_lines.append(line)

        process.wait()
        if process.returncode != 0:
            raise Exception("\n".join(stdout_lines))

        resumo = {}
        for line in stdout_lines:
            if line.startswith("{") and line.endswith("}"):
                try:
                    resumo = json.loads(line)
                    break
                except Exception:
                    continue

        input_id = resumo.get("input_id")
        validos = resumo.get("validos", 0)
        invalidos = resumo.get("invalidos", 0)
        total_processados = resumo.get("total_processados", validos + invalidos)
        arquivo_invalidos = resumo.get("arquivo_invalidos")
        arquivo_nome = resumo.get("arquivo")
        status = resumo.get("status", "done")

        logger.info(
            f"✅ Job {job_id} concluído (input_id={input_id}) → {validos} válidos / {invalidos} inválidos"
        )

        job.meta["step"] = "Finalizado"
        job.meta["progress"] = 100
        job.save_meta()

        salvar_historico(
            tenant_id,
            input_id,
            descricao,
            status,
            arquivo_nome,
            total_processados,
            validos,
            invalidos,
            arquivo_invalidos,
            "✅ Pré-processamento concluído com sucesso",
        )

        return {
            "status": status,
            "job_id": job_id,
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao,
            "arquivo": arquivo_nome,
            "total_processados": total_processados,
            "validos": validos,
            "invalidos": invalidos,
            "arquivo_invalidos": arquivo_invalidos,
        }

    except Exception as e:
        logger.error(f"❌ Erro no job {job_id}: {e}", exc_info=True)
        job.meta["step"] = "Erro"
        job.meta["progress"] = 100
        job.save_meta()

        # tenta capturar input_id se já tiver sido gerado
        try:
            input_id = resumo.get("input_id", None)
        except Exception:
            input_id = None

        salvar_historico(
            tenant_id,
            input_id,
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
            "input_id": input_id,
            "descricao": descricao,
            "error": str(e),
        }

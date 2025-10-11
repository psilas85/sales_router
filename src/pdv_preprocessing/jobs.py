#sales_router/src/pdv_preprocessing/jobs.py

import logging
import subprocess
import re
import json
from datetime import datetime
from rq import get_current_job
from database.db_connection import get_connection  # ✅ usa o core global

logger = logging.getLogger(__name__)


# ============================================================
# 🧾 Função: salvar histórico no banco
# ============================================================
def salvar_historico(tenant_id, job_id, status, arquivo, total, validos, invalidos, arquivo_invalidos, mensagem):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO historico_pdv_jobs
            (tenant_id, job_id, arquivo, status, total_processados, validos, invalidos, arquivo_invalidos, mensagem, criado_em)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                tenant_id,
                job_id,
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
        logger.info(f"💾 Histórico gravado para job {job_id}")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar histórico no banco: {e}", exc_info=True)


# ============================================================
# 🚀 Função principal: processar CSV via subprocesso
# ============================================================
def processar_csv(job_id, tenant_id, file_path, modo_forcar=False):
    job = get_current_job()
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
        ]

        if modo_forcar:
            comando.append("--modo_forcar")

        job.meta["step"] = "Iniciando processamento"
        job.meta["progress"] = 0
        job.save_meta()

        logger.info(f"▶️ Executando comando: {' '.join(comando)}")
        job.meta["step"] = "Executando pipeline"
        job.meta["progress"] = 20
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

        validos, invalidos, total_processados = 0, 0, 0
        arquivo_invalidos = None

        job.meta["step"] = "Interpretando saída"
        job.meta["progress"] = 70
        job.save_meta()

        # 🔎 JSON principal (stdout)
        for line in stdout_lines:
            if line.startswith("{") and line.endswith("}"):
                try:
                    resumo = json.loads(line)
                    validos = resumo.get("validos", 0)
                    invalidos = resumo.get("invalidos", 0)
                    total_processados = resumo.get("total_processados", validos + invalidos)
                    arquivo_invalidos = resumo.get("arquivo_invalidos")
                    break
                except Exception:
                    pass

        # 🔎 Regex fallback (caso JSON falhe)
        if total_processados == 0:
            for line in stdout_lines:
                match = re.search(r"(\d+)\s+PDVs válid.*?(\d+)\s+inválid", line, re.IGNORECASE)
                if match:
                    validos, invalidos = int(match.group(1)), int(match.group(2))
                    total_processados = validos + invalidos
                    break

        logger.info(f"✅ Job {job_id} concluído: {validos} válidos, {invalidos} inválidos")

        job.meta["step"] = "Finalizado"
        job.meta["progress"] = 100
        job.save_meta()

        salvar_historico(
            tenant_id,
            job_id,
            "done",
            file_path.split("/")[-1],
            total_processados,
            validos,
            invalidos,
            arquivo_invalidos,
            "✅ Pré-processamento de PDVs concluído com sucesso",
        )

        return {
            "status": "done",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "arquivo": file_path.split("/")[-1],
            "total_processados": total_processados,
            "validos": validos,
            "invalidos": invalidos,
            "arquivo_invalidos": arquivo_invalidos,
            "mensagem": "✅ Pré-processamento de PDVs concluído com sucesso",
        }

    except Exception as e:
        logger.error(f"❌ Erro no job {job_id}: {e}", exc_info=True)
        job.meta["step"] = "Erro"
        job.meta["progress"] = 100
        job.save_meta()

        salvar_historico(
            tenant_id,
            job_id,
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
            "error": str(e),
        }

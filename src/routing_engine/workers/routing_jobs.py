# sales_router/src/routing_engine/workers/routing_jobs.py

import subprocess
import json
import sys
import logging
from rq import get_current_job
from uuid import uuid4
import os

logger = logging.getLogger("routing_jobs")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def processar_routing(file_path):

    job = get_current_job()
    job_id = str(job.id) if job and job.id else str(uuid4())

    OUTPUT_DIR = "/app/data/routing_outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_file = f"{OUTPUT_DIR}/{job_id}.xlsx"

    logger.info(f"🚀 Routing job iniciado | job_id={job_id}")

    # =========================================================
    # INIT META
    # =========================================================
    if job:
        job.meta["output_file"] = output_file
        job.meta["progress"] = 0
        job.meta["step"] = "Inicializando"
        job.save_meta()

    # =========================================================
    # COMANDO
    # =========================================================
    comando = [
        "python3",
        "-u",
        "src/routing_engine/main_routing_spreadsheet.py",
        file_path,
        output_file
    ]

    logger.info(f"▶️ Executando: {' '.join(comando)}")

    proc = subprocess.Popen(
        comando,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )

    resumo = None

    # =========================================================
    # LEITURA DO SUBPROCESS (COM PROGRESS JSON)
    # =========================================================
    if proc.stdout:
        for line in proc.stdout:

            line = line.strip()

            if not line:
                continue

            # LOG normal
            if not line.startswith("{"):
                logger.info(f"[MAIN] {line}")
                continue

            # JSON estruturado
            try:

                obj = json.loads(line)

                # -----------------------------------------
                # PROGRESS
                # -----------------------------------------
                if obj.get("event") == "progress":

                    if job:
                        job.meta.update({
                            "progress": obj.get("pct"),
                            "step": obj.get("step")
                        })
                        job.save_meta()

                    continue

                # -----------------------------------------
                # RESULTADO FINAL
                # -----------------------------------------
                if obj.get("status"):
                    resumo = obj

            except Exception:
                logger.warning(f"[JSON_INVALIDO] {line}")

    proc.wait()

    # =========================================================
    # ERRO
    # =========================================================
    if proc.returncode != 0:
        logger.error(f"❌ subprocess retornou código {proc.returncode}")

        if job:
            job.meta["step"] = "Erro"
            job.meta["progress"] = 100
            job.save_meta()

        raise RuntimeError("Routing subprocess falhou")

    logger.info(f"✅ Routing job finalizado | job_id={job_id}")

    # =========================================================
    # FINAL META
    # =========================================================
    if job:
        job.meta["progress"] = 100
        job.meta["step"] = "Finalizado"
        job.save_meta()

    # =========================================================
    # PADRONIZA RESULTADO
    # =========================================================
    if resumo is None:
        resumo = {
            "status": "done",
            "output_file": output_file
        }

    else:
        resumo["output_file"] = output_file

    return resumo
#sales_router/src/geocoding_engine/workers/geocode_jobs.py

import subprocess
import json
import sys
import logging

from rq import get_current_job
from uuid import uuid4
import os

# ============================================================
# LOGGER
# ============================================================

logger = logging.getLogger("geocode_jobs")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# ============================================================
# WORKER JOB
# ============================================================

def processar_geocode(file_path):

    job = get_current_job()

    job_id = str(job.id) if job and job.id else str(uuid4())

    OUTPUT_DIR = "/app/data/geocode_outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_file = f"{OUTPUT_DIR}/{job_id}.xlsx"

    logger.info(f"🚀 Geocode job iniciado | job_id={job_id}")

    comando = [
        "python3",
        "-u",
        "src/geocoding_engine/main_geocode_spreadsheet.py",
        "--arquivo",
        file_path,
        "--saida",
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

    # ========================================================
    # STREAM DO SUBPROCESS
    # ========================================================

    if proc.stdout:

        for line in proc.stdout:

            line = line.strip()

            if not line:
                continue

            # Logs normais
            if not line.startswith("{"):
                logger.info(f"[MAIN] {line}")
                continue

            try:

                obj = json.loads(line)

                # -----------------------------
                # progresso
                # -----------------------------
                if obj.get("event") == "progress":

                    if job:
                        job.meta.update({
                            "progress": obj.get("pct"),
                            "step": obj.get("step")
                        })
                        job.save_meta()

                    continue

                # -----------------------------
                # resultado final
                # -----------------------------
                if obj.get("status"):
                    resumo = obj

            except Exception:
                logger.warning(f"[JSON_INVALIDO] {line}")

    proc.wait()

    # ========================================================
    # ERRO NO PROCESSO
    # ========================================================

    if proc.returncode != 0:
        logger.error(f"❌ subprocess retornou código {proc.returncode}")
        raise RuntimeError("Geocode subprocess falhou")

    logger.info(f"✅ Geocode job finalizado | job_id={job_id}")

    if job:
        job.meta["output_file"] = output_file
        job.save_meta()

    return resumo
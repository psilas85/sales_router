#sales_router/src/geocoding_engine/workers/geocode_jobs.py

import subprocess
import json
import sys
import logging
from rq import get_current_job
from uuid import uuid4
import os

logger = logging.getLogger("geocode_jobs")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


def processar_geocode(file_path):

    job = get_current_job()
    job_id = str(job.id) if job and job.id else str(uuid4())

    OUTPUT_DIR = "/app/data/geocode_outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_file = f"{OUTPUT_DIR}/{job_id}.xlsx"
    output_json = f"{OUTPUT_DIR}/{job_id}.json"

    logger.info(f"🚀 Geocode job iniciado | job_id={job_id}")

    if job:
        job.meta["output_file"] = output_file
        job.meta["output_json"] = output_json
        job.meta["progress"] = 0
        job.meta["step"] = "Inicializando"
        job.save_meta()

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

    if proc.stdout:
        for line in proc.stdout:

            line = line.strip()

            if not line:
                continue

            if not line.startswith("{"):
                logger.info(f"[MAIN] {line}")
                continue

            try:

                obj = json.loads(line)

                if obj.get("event") == "progress":

                    if job:
                        job.meta.update({
                            "progress": obj.get("pct"),
                            "step": obj.get("step")
                        })
                        job.save_meta()

                    continue

                if obj.get("status"):
                    resumo = obj

            except Exception:
                logger.warning(f"[JSON_INVALIDO] {line}")

    proc.wait()

    if proc.returncode != 0:
        logger.error(f"❌ subprocess retornou código {proc.returncode}")
        raise RuntimeError("Geocode subprocess falhou")

    logger.info(f"✅ Geocode job finalizado | job_id={job_id}")

    # =========================================================
    # GERAR JSON PARA MAPA
    # =========================================================

    import pandas as pd
    import numpy as np

    try:

        df = pd.read_excel(output_file, sheet_name="geocodificados")

    except Exception as e:

        logger.error(f"Erro lendo aba geocodificados: {e}")
        return resumo


    lat_cols = ["lat", "latitude", "Latitude", "LAT"]
    lon_cols = ["lon", "lng", "longitude", "Longitude", "LON"]

    lat_col = next((c for c in lat_cols if c in df.columns), None)
    lon_col = next((c for c in lon_cols if c in df.columns), None)

    if not lat_col or not lon_col:

        logger.error("Colunas lat/lon não encontradas na planilha")
        return resumo


    # ---------------------------------------------------------
    # CONVERSÃO NUMÉRICA
    # ---------------------------------------------------------

    df[lat_col] = pd.to_numeric(df[lat_col], errors="coerce")
    df[lon_col] = pd.to_numeric(df[lon_col], errors="coerce")


    # ---------------------------------------------------------
    # REMOVER VALORES INVÁLIDOS
    # ---------------------------------------------------------

    df = df.dropna(subset=[lat_col, lon_col])

    df = df[
        np.isfinite(df[lat_col]) &
        np.isfinite(df[lon_col])
    ]


    # ---------------------------------------------------------
    # CONSTRUIR DATAFRAME DE SAÍDA
    # ---------------------------------------------------------

    result = df[[lat_col, lon_col]].copy()

    result.rename(
        columns={
            lat_col: "lat",
            lon_col: "lon"
        },
        inplace=True
    )


    # CAMPOS OPCIONAIS

    if "cidade" in df.columns:
        result["cidade"] = df["cidade"]

    if "setor" in df.columns:
        result["setor"] = df["setor"]

    if "logradouro" in df.columns:
        result["endereco"] = df["logradouro"]


    # ---------------------------------------------------------
    # LIMPEZA PARA JSON
    # ---------------------------------------------------------

    result = result.replace(
        [np.nan, np.inf, -np.inf],
        None
    )

    records = result.to_dict(orient="records")

    clean_records = []

    for r in records:

        clean = {}

        for k, v in r.items():

            if isinstance(v, float):

                if np.isnan(v) or np.isinf(v):
                    clean[k] = None
                else:
                    clean[k] = float(v)

            else:
                clean[k] = v

        clean_records.append(clean)


    # ---------------------------------------------------------
    # SALVAR JSON
    # ---------------------------------------------------------

    with open(output_json, "w", encoding="utf-8") as f:

        json.dump(
            clean_records,
            f,
            ensure_ascii=False
        )

    logger.info(
        f"JSON de mapa gerado com {len(clean_records)} pontos"
    )


    # =========================================================
    # RETORNAR RESUMO PADRONIZADO PARA RQ
    # =========================================================

    if resumo is None:

        resumo = {
            "status": "done",
            "total": len(clean_records),
            "sucesso": len(clean_records),
            "falhas": 0
        }

    else:

        # quando subprocess retorna stats
        if "stats" in resumo:

            stats = resumo.get("stats", {})

            resumo = {
                "status": resumo.get("status", "done"),
                "total": stats.get("total", 0),
                "sucesso": stats.get("sucesso", 0),
                "falhas": stats.get("falhas", 0)
            }

        else:

            resumo = {
                "status": resumo.get("status", "done"),
                "total": resumo.get("total", 0),
                "sucesso": resumo.get("sucesso", 0),
                "falhas": resumo.get("falhas", 0)
            }

    return resumo
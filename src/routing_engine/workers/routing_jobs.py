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


# =========================================================
# 🔥 NORMALIZAÇÃO DE ROTAS (VERSÃO FINAL)
# =========================================================
def normalizar_rotas(rotas_raw):

    rotas_processadas = []

    for rota in rotas_raw:

        # 🔥 PRIORIDADE: rota real (OSRM/Google)
        rota_geom = rota.get("rota_coord")

        if rota_geom and isinstance(rota_geom, list):

            coords_final = [
                {
                    "lat": p.get("lat"),
                    "lon": p.get("lon")
                }
                for p in rota_geom
                if p.get("lat") is not None and p.get("lon") is not None
            ]

        else:
            # fallback: pontos simples
            coords = rota.get("coords", [])

            if coords and isinstance(coords[0], dict):

                coords_final = [
                    {
                        "lat": p.get("lat"),
                        "lon": p.get("lon"),
                        "ordem": p.get("ordem", i)
                    }
                    for i, p in enumerate(coords)
                    if p.get("lat") is not None and p.get("lon") is not None
                ]

            else:

                coords_final = [
                    {
                        "lat": c[0],
                        "lon": c[1],
                        "ordem": i
                    }
                    for i, c in enumerate(coords)
                    if c and len(c) == 2 and c[0] is not None and c[1] is not None
                ]

        if len(coords_final) < 2:
            continue

        rotas_processadas.append({
            "rota_id": rota.get("rota_id"),
            "cluster": rota.get("cluster"),
            "veiculo": rota.get("veiculo"),
            "coords": coords_final
        })

    logger.info(f"🧭 Rotas normalizadas: {len(rotas_processadas)}")

    return rotas_processadas


# =========================================================
# 🚀 PROCESSAMENTO PRINCIPAL
# =========================================================
def processar_routing(file_path, params=None):

    params = params or {}

    job = get_current_job()
    job_id = str(job.id) if job and job.id else str(uuid4())

    OUTPUT_DIR = "/app/data/routing_outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_file = f"{OUTPUT_DIR}/{job_id}.xlsx"
    output_json = f"{OUTPUT_DIR}/{job_id}.json"

    logger.info(f"🚀 Routing job iniciado | job_id={job_id}")
    logger.info(f"⚙️ Params recebidos: {params}")

    if job:
        job.meta.update({
            "output_file": output_file,
            "output_json": output_json,
            "progress": 0,
            "step": "Inicializando"
        })
        job.save_meta()

    params_str = json.dumps(params)

    comando = [
        "python3",
        "-u",
        "/app/src/routing_engine/main_routing_spreadsheet.py",
        file_path,
        output_file,
        params_str
    ]

    logger.info(f"▶️ Executando: {' '.join(comando)}")

    proc = subprocess.Popen(
        comando,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        encoding="utf-8",
        bufsize=1
    )

    resumo = None
    rotas = []
    line_buffer = []

    if proc.stdout:
        for line in iter(proc.stdout.readline, ""):

            line = line.strip()

            if not line:
                continue

            line_buffer.append(line)

            if not line.startswith("{"):
                logger.info(f"[MAIN] {line}")
                continue

            try:
                obj = json.loads(line)

                if obj.get("event") == "progress":
                    if job:
                        job.meta.update({
                            "progress": obj.get("pct", 0),
                            "step": obj.get("step", "")
                        })
                        job.save_meta()
                    continue

                if obj.get("event") == "routes":
                    rotas = obj.get("data", [])
                    logger.info(f"📦 Rotas recebidas: {len(rotas)}")
                    continue

                if obj.get("status"):
                    resumo = obj

            except Exception:
                logger.warning(f"[JSON_INVALIDO] {line}")

    proc.wait()

    if proc.returncode != 0:

        error_message = None

        # 🔥 pega a última mensagem relevante
        for line in reversed(line_buffer):

            if "ValueError" in line or "Exception" in line:
                error_message = line
                break

        # fallback
        if not error_message and line_buffer:
            error_message = line_buffer[-1]

        if not error_message:
            error_message = "Erro no processamento de roteirização"

        # 🔥 limpa mensagem (remove prefixos feios)
        error_message = (
            error_message
            .replace("ValueError:", "")
            .replace("Exception:", "")
            .strip()
        )

        if job:
            job.meta["error"] = {
                "mensagem": error_message,
                "tipo": "VALIDACAO_DADOS" if "ValueError" in error_message else "EXECUCAO",
                "detalhes": None,
                "subjobs_falhados": job.meta.get("subjob_errors")
            }

            job.meta["error_debug"] = line_buffer[-50:] if line_buffer else []
            job.save_meta()

        raise RuntimeError(error_message)

    logger.info(f"✅ Routing job finalizado | job_id={job_id}")

    rotas = normalizar_rotas(rotas)

    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(rotas, f, ensure_ascii=False, indent=2)

    logger.info(f"🗺️ JSON de rotas salvo: {output_json}")

    metricas = {}

    if resumo and isinstance(resumo.get("output"), dict):
        metricas = resumo["output"].get("metricas", {})

    summary = {
        "total_pdvs_validos": metricas.get("total_pdvs_validos"),
        "total_pdvs_invalidos": metricas.get("total_pdvs_invalidos"),
        "total_grupos_validos": metricas.get("total_grupos_validos"),
        "grupos_processados": metricas.get("grupos_processados"),
        "grupos_com_erro": metricas.get("grupos_com_erro"),
        "total_rotas": metricas.get("total_rotas"),
        "taxa_sucesso": metricas.get("taxa_sucesso"),
        "tempo_execucao_ms": metricas.get("tempo_execucao_ms"),
        "cache_hits": metricas.get("cache_hits"),
        "osrm_hits": metricas.get("osrm_hits"),
        "google_hits": metricas.get("google_hits"),
        "haversine_hits": metricas.get("haversine_hits"),
        "validacao": metricas.get("validacao"),
    }
    if job:
        job.meta.update({
            "progress": 100,
            "step": "Finalizado",
            "output_json": output_json,
            "summary": summary   # 🔥 AGORA VAI FUNCIONAR
        })
        job.save_meta()

    if resumo is None:
        resumo = {"status": "done"}

    resumo.update({
        "output_file": output_file,
        "output_json": output_json
    })

    return resumo
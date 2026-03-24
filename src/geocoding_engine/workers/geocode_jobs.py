#sales_router/src/geocoding_engine/workers/geocode_jobs.py

import json
import sys
import logging
from rq import get_current_job
from uuid import uuid4
import os
import time

import pandas as pd
import numpy as np
import redis

from rq import Queue
from rq.job import Job

logger = logging.getLogger("geocode_jobs")
logger.setLevel(logging.INFO)

if not logger.handlers:
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    handler.setFormatter(formatter)
    logger.addHandler(handler)


# =========================================================
# HELPERS
# =========================================================

def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def montar_addresses(df: pd.DataFrame):
    addresses = []

    for idx, row in df.iterrows():
        logradouro = row.get("logradouro")
        numero = row.get("numero")
        bairro = row.get("bairro")
        cidade = row.get("cidade")
        uf = row.get("uf")

        partes = []

        if pd.notna(logradouro):
            partes.append(str(logradouro).strip())

        if pd.notna(numero) and str(numero).strip():
            if partes:
                partes[-1] = f"{partes[-1]} {str(numero).strip()}"

        if pd.notna(bairro) and str(bairro).strip():
            partes.append(str(bairro).strip())

        if pd.notna(cidade) and pd.notna(uf):
            partes.append(f"{str(cidade).strip()} - {str(uf).strip()}")

        endereco = ", ".join(partes)

        addresses.append({
            "id": int(idx),
            "address": endereco,
            "cidade": None if pd.isna(cidade) else str(cidade).strip(),
            "uf": None if pd.isna(uf) else str(uf).strip()
        })

    return addresses


# =========================================================
# JOB PRINCIPAL
# =========================================================

def processar_geocode(file_path):

    job = get_current_job()
    job_id = str(job.id) if job and job.id else str(uuid4())

    OUTPUT_DIR = "/app/data/geocode_outputs"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    output_file = f"{OUTPUT_DIR}/{job_id}.xlsx"
    output_json = f"{OUTPUT_DIR}/{job_id}.json"

    logger.info(f"🚀 Geocode job iniciado | job_id={job_id}")

    redis_conn = redis.Redis(host="redis", port=6379)  # RQ (SEM decode)
    redis_str = redis.Redis(host="redis", port=6379, decode_responses=True)  # RESULTADOS
    subjob_queue = Queue("geocode_subjobs", connection=redis_conn)

    CHUNK_SIZE = 200

    if job:
        job.meta.update({
            "output_file": output_file,
            "output_json": output_json,
            "progress": 0,
            "step": "Inicializando"
        })
        job.save_meta()

    try:
        # =====================================================
        # LEITURA
        # =====================================================
        if job:
            job.meta["progress"] = 5
            job.meta["step"] = "Lendo arquivo"
            job.save_meta()

        df = pd.read_excel(file_path)

        # 🔥 GARANTIR COLUNAS PADRÃO
        for col in ["setor", "consultor", "cnpj", "razao_social", "nome_fantasia"]:
            if col not in df.columns:
                df[col] = None

        # =====================================================
        # PREPARAÇÃO
        # =====================================================
        if job:
            job.meta["progress"] = 10
            job.meta["step"] = "Preparando endereços"
            job.save_meta()

        addresses = montar_addresses(df)

        if not addresses:
            raise ValueError("Nenhum endereço encontrado")

        # =====================================================
        # CHUNKS
        # =====================================================
        chunks = list(chunk_list(addresses, CHUNK_SIZE))
        total_chunks = len(chunks)

        logger.info(f"📦 {total_chunks} chunks criados")

        redis_key = f"geocode_result:{job_id}"
        redis_str.delete(redis_key)

        # =====================================================
        # ENQUEUE
        # =====================================================
        if job:
            job.meta["progress"] = 15
            job.meta["step"] = "Enfileirando subjobs"
            job.save_meta()

        subjob_ids = []

        for chunk_id, chunk in enumerate(chunks):
            payload = {
                "chunk_id": chunk_id,
                "parent_id": job_id,
                "addresses": chunk
            }

            subjob = subjob_queue.enqueue(
                "geocoding_engine.workers.geocode_subjob.processar_subjob",
                payload,
                job_timeout=1800
            )

            subjob_ids.append(subjob.id)

        logger.info(f"🚀 {len(subjob_ids)} subjobs enviados")

        # =====================================================
        # AGUARDAR
        # =====================================================
        while True:
            finished = 0
            failed = 0

            for jid in subjob_ids:
                j = Job.fetch(jid, connection=redis_conn)

                if j.is_finished:
                    finished += 1
                elif j.is_failed:
                    failed += 1

            if failed > 0:
                raise RuntimeError(f"{failed} subjob(s) falharam")

            progress = 20 + int((finished / total_chunks) * 60) if total_chunks else 80

            if job:
                job.meta["progress"] = progress
                job.meta["step"] = f"Processando ({finished}/{total_chunks})"
                job.save_meta()

            if finished == total_chunks:
                break

            time.sleep(1)

        # =====================================================
        # CONSOLIDAR
        # =====================================================
        if job:
            job.meta["progress"] = 85
            job.meta["step"] = "Consolidando resultados"
            job.save_meta()

        raw = redis_str.lrange(redis_key, 0, -1)

        final = []
        for item in raw:
            data = json.loads(item)
            final.extend(data["results"])

        df_result = pd.DataFrame(final)

        df_result = df_result.sort_values("id").drop_duplicates("id")

        df["id"] = df.index.astype(int)

        df_final = df.merge(
            df_result[["id", "lat", "lon", "source"]],
            on="id",
            how="left"
        )

        # =====================================================
        # SALVAR
        # =====================================================
        if job:
            job.meta["progress"] = 90
            job.meta["step"] = "Salvando"
            job.save_meta()

        # 🔥 GARANTIR ORDEM E PRESENÇA DAS COLUNAS
        colunas_padrao = [
            "cnpj", "razao_social", "nome_fantasia",
            "logradouro", "numero", "bairro", "cidade", "uf", "cep",
            "consultor", "setor",
            "id", "lat", "lon", "source"
        ]

        for col in colunas_padrao:
            if col not in df_final.columns:
                df_final[col] = None

        df_final = df_final[colunas_padrao]

        # 🔥 SEPARAR VALIDOS / INVALIDOS
        df_validos = df_final.dropna(subset=["lat", "lon"]).copy()
        df_invalidos = df_final[df_final["lat"].isnull()].copy()

        with pd.ExcelWriter(output_file, engine="openpyxl") as writer:
            df_validos.to_excel(writer, sheet_name="geocodificados", index=False)
            df_invalidos.to_excel(writer, sheet_name="invalidos", index=False)

        logger.info(f"✅ Geocode job finalizado")

    except Exception as e:
        logger.error(f"❌ erro: {e}")
        raise

    finally:
        redis_str.delete(redis_key)

    # =====================================================
    # JSON MAPA + RESUMO
    # =====================================================

    # 🔥 lê resultado do Excel
    df_validos = pd.read_excel(output_file, sheet_name="geocodificados")
    df_invalidos = pd.read_excel(output_file, sheet_name="invalidos")

    # 🔥 métricas corretas
    total = len(df_validos) + len(df_invalidos)
    sucesso = len(df_validos)
    falhas = len(df_invalidos)

    # 🔥 usa SOMENTE válidos para o mapa
    result = df_validos[["lat", "lon"]].copy()

    # mantém cidade se existir
    if "cidade" in df_validos.columns:
        result["cidade"] = df_validos["cidade"]

    records = result.to_dict(orient="records")

    # 🔥 salva JSON do mapa
    with open(output_json, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False)
   

    # 🔥 SALVAR HISTÓRICO
    from geocoding_engine.infrastructure.geocoding_history_repository import GeocodingHistoryRepository
    from geocoding_engine.infrastructure.database_reader import DatabaseReader

    reader = DatabaseReader()
    conn = reader.conn

    repo = GeocodingHistoryRepository(conn)

    repo.salvar(
        request_id=job_id,
        tenant_id=1,
        origem="job_distribuido",
        total=total,
        sucesso=sucesso,
        falhas=falhas,
        cache_hits=0,
        nominatim_hits=0,
        google_hits=0,
        tempo_ms=0
    )

    # 🔥 META FINAL
    if job:
        job.meta.update({
            "progress": 100,
            "step": "Concluído",
            "result": {
                "total": int(total),
                "sucesso": int(sucesso),
                "falhas": int(falhas)
            }
        })
        job.save_meta()

    return {
        "status": "done",
        "total": int(total),
        "sucesso": int(sucesso),
        "falhas": int(falhas)
    }
        
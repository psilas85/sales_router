#sales_router/src/sales_clusterization/api/routes.py

# ============================================================
# 📦 src/sales_clusterization/api/routes.py
# ============================================================

from fastapi import APIRouter, Query, Depends, Request, HTTPException
from loguru import logger
from redis import Redis
from rq import Queue
from rq.job import Job
from sales_clusterization.reporting.export_cluster_resumo_xlsx import exportar_cluster_resumo
from sales_clusterization.reporting.export_cluster_pdv_detalhado_xlsx import exportar_cluster_pdv_detalhado
from sales_clusterization.visualization.cluster_plotting import buscar_run_por_clusterization_id, buscar_clusters, gerar_mapa_clusters
from .dependencies import verify_token
from pathlib import Path
import os

router = APIRouter()

# ============================================================
# 🧠 Health
# ============================================================
@router.get("/health", tags=["Status"])
def health():
    return {"status": "ok", "message": "Clusterization API saudável 🧩"}

# ============================================================
# 🚀 Enfileirar clusterização
# ============================================================
@router.post("/clusterizar", dependencies=[Depends(verify_token)], tags=["Clusterização"])
def clusterizar(
    request: Request,
    uf: str = Query(...),
    descricao: str = Query(...),
    input_id: str = Query(...),
    cidade: str = Query(None),
    algo: str = Query("kmeans"),
    max_pdv_cluster: int = Query(200)
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        redis_conn = Redis(host="redis", port=6379)
        queue = Queue("clusterization_jobs", connection=redis_conn)

        params = {
            "tenant_id": tenant_id,
            "uf": uf,
            "cidade": cidade,
            "algo": algo,
            "descricao": descricao,
            "input_id": input_id,
            "max_pdv_cluster": max_pdv_cluster,
            "usuario": user["email"],
        }

        job = queue.enqueue("src.jobs.clusterization_jobs.executar_clusterization_job",
                            params,
                            job_timeout=1800)

        logger.info(f"🚀 Job clusterização enfileirado: {job.id} | tenant={tenant_id}")

        return {
            "status": "queued",
            "job_id": job.id,
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao
        }

    except Exception as e:
        logger.error(f"❌ Erro ao enfileirar clusterização: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 📋 Listar jobs
# ============================================================
@router.get("/jobs", dependencies=[Depends(verify_token)], tags=["Clusterização"])
def listar_jobs(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    conn = get_connection()
    df = pd.read_sql(f"""
        SELECT * 
        FROM historico_cluster_jobs
        WHERE tenant_id={tenant_id}
        ORDER BY criado_em DESC
        LIMIT 100;
    """, conn)
    conn.close()

    df = df.astype(object).replace({np.nan: None})

    return {"total": len(df), "jobs": df.to_dict(orient="records")}


# ============================================================
# 🔍 Detalhar job
# ============================================================
@router.get("/jobs/{job_id}", dependencies=[Depends(verify_token)], tags=["Clusterização"])
def detalhar_job(job_id: str):
    try:
        conn = Redis(host="redis", port=6379)
        job = Job.fetch(job_id, connection=conn)
        return {
            "job_id": job.id,
            "status": job.get_status(),
            "meta": job.meta,
            "params": job.args
        }
    except:
        raise HTTPException(status_code=404, detail="Job não encontrado.")


# ============================================================
# 📊 Progresso
# ============================================================
@router.get("/jobs/{job_id}/progress", dependencies=[Depends(verify_token)], tags=["Clusterização"])
def progresso(job_id: str):
    try:
        conn = Redis(host="redis", port=6379)
        job = Job.fetch(job_id, connection=conn)
        return {
            "job_id": job.id,
            "status": job.get_status(),
            "meta": job.meta
        }
    except:
        raise HTTPException(status_code=404, detail="Job não encontrado.")


# ============================================================
# 📊 Exportar resumo XLSX
# ============================================================
@router.get("/export/resumo", dependencies=[Depends(verify_token)], tags=["Relatórios"])
def export_resumo(request: Request, clusterization_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        exportar_cluster_resumo(tenant_id, clusterization_id)
        path = f"output/reports/{tenant_id}/cluster_resumo_{clusterization_id}.xlsx"
        return {"status": "success", "arquivo": path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 📊 Exportar detalhado XLSX
# ============================================================
@router.get("/export/detalhado", dependencies=[Depends(verify_token)], tags=["Relatórios"])
def export_detalhado(request: Request, clusterization_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        exportar_cluster_pdv_detalhado(tenant_id, clusterization_id)
        path = f"output/reports/{tenant_id}/cluster_pdv_detalhado_{clusterization_id}.xlsx"
        return {"status": "success", "arquivo": path}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 🗺️ Gerar mapa
# ============================================================
@router.post("/mapa", dependencies=[Depends(verify_token)], tags=["Visualização"])
def gerar_mapa(request: Request, clusterization_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        run_id = buscar_run_por_clusterization_id(tenant_id, clusterization_id)
        if not run_id:
            raise HTTPException(404, "Run não encontrado.")

        dados = buscar_clusters(tenant_id, run_id)

        output_dir = Path(f"/app/output/maps/{tenant_id}")
        output_dir.mkdir(parents=True, exist_ok=True)

        output_path = output_dir / f"clusterization_{clusterization_id}.html"

        gerar_mapa_clusters(dados, output_path)

        return {
            "status": "success",
            "arquivo_html": str(output_path),
            "url_relativa": f"/output/maps/{tenant_id}/clusterization_{clusterization_id}.html"
        }

    except Exception as e:
        raise HTTPException(500, str(e))

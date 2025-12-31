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
from sales_clusterization.visualization.cluster_plotting import (
    buscar_run_por_clusterization_id,
    buscar_clusters,
    gerar_mapa_clusters,
)
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
# 📦 Listar Inputs disponíveis para Clusterização
# ============================================================
# ============================================================
# 📦 Listar Inputs disponíveis para Clusterização (PAGINADO)
# ============================================================
@router.get("/inputs", dependencies=[Depends(verify_token)])
def listar_inputs(
    request: Request,
    limit: int = Query(5, ge=1, le=100),
    offset: int = Query(0, ge=0),
    data_inicio: str | None = Query(None),
    data_fim: str | None = Query(None),
    descricao: str | None = Query(None),
):
    """
    Retorna inputs disponíveis para clusterização:
    - paginado
    - com filtros
    - com total REAL (para paginação correta no frontend)
    """

    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    conn = get_connection()

    # ============================================================
    # TOTAL (COM OS MESMOS FILTROS DO SELECT PRINCIPAL)
    # ============================================================
    sql_total = """
        SELECT COUNT(DISTINCT h.input_id)
        FROM historico_pdv_jobs h
        WHERE h.tenant_id = %s
          AND h.status = 'done'
          AND (%s IS NULL OR DATE(h.criado_em) >= %s)
          AND (%s IS NULL OR DATE(h.criado_em) <= %s)
          AND (%s IS NULL OR LOWER(h.descricao) LIKE %s)
    """

    total = pd.read_sql_query(
        sql_total,
        conn,
        params=(
            tenant_id,
            data_inicio, data_inicio,
            data_fim, data_fim,
            descricao, f"%{descricao.lower()}%" if descricao else None,
        ),

    ).iloc[0, 0]

    # ============================================================
    # DADOS PAGINADOS
    # ============================================================
    sql = """
        SELECT
            h.input_id,
            h.criado_em,
            h.descricao,
            MIN(p.uf)      AS uf,
            MIN(p.cidade)  AS cidade,
            COUNT(p.id)    AS total_pdvs
        FROM historico_pdv_jobs h
        LEFT JOIN pdvs p
          ON p.tenant_id = h.tenant_id
         AND p.input_id  = h.input_id
        WHERE h.tenant_id = %s
          AND h.status = 'done'
          AND (%s IS NULL OR DATE(h.criado_em) >= %s)
          AND (%s IS NULL OR DATE(h.criado_em) <= %s)
          AND (%s IS NULL OR LOWER(h.descricao) LIKE %s)
        GROUP BY h.input_id, h.criado_em, h.descricao
        ORDER BY h.criado_em DESC
        LIMIT %s OFFSET %s
    """

    df = pd.read_sql_query(
        sql,
        conn,
        params=(
            tenant_id,
            data_inicio, data_inicio,
            data_fim, data_fim,
            descricao, f"%{descricao.lower()}%" if descricao else None,
            limit,
            offset,
        )

    )

    conn.close()

    df = df.astype(object).replace({np.nan: None})

    return {
        "total": int(total),
        "inputs": df.to_dict(orient="records"),
    }



# ============================================================
# 🚀 Enfileirar clusterização
# ============================================================
@router.post("/clusterizar", dependencies=[Depends(verify_token)], tags=["Clusterização"])
async def clusterizar(request: Request):

    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        body = await request.json()
    except:
        raise HTTPException(400, "JSON inválido no body.")

    params = {
        "tenant_id": tenant_id,
        "uf": body["uf"],
        "cidade": body.get("cidade"),
        "algo": body.get("algo", "kmeans"),
        "descricao": body["descricao"],
        "input_id": body["input_id"],

        "max_pdv_cluster": body.get("max_pdv_cluster", 200),
        "dias_uteis": body.get("dias_uteis", 20),
        "freq": body.get("freq", 1),
        "workday_min": body.get("workday_min", 500),
        "route_km_max": body.get("route_km_max", 200),
        "service_min": body.get("service_min", 30),
        "v_kmh": body.get("v_kmh", 35),
        "alpha_path": body.get("alpha_path", 1.4),

        "excluir_outliers": body.get("excluir_outliers", False),
        "z_thresh": body.get("z_thresh", 3.0),

        "k_forcado": body.get("k_forcado"),
        "usuario": user["email"],
        "max_iter": body.get("max_iter", 10),
    }

    redis_conn = Redis(host="redis", port=6379)
    queue = Queue("clusterization_jobs", connection=redis_conn)

    job = queue.enqueue(
        "src.jobs.tasks.clusterization_task.executar_clusterization_job",
        params,
        job_timeout=1800,
    )

    return {"status": "queued", "job_id": job.id}

# ============================================================
# 📋 Listar jobs
# ============================================================
@router.get("/jobs", dependencies=[Depends(verify_token)], tags=["Clusterização"])
def listar_jobs(
    request: Request,
    data_inicio: str | None = Query(default=None),
    data_fim: str | None = Query(default=None),
    descricao: str | None = Query(default=None),
    limit: int = Query(default=5, le=500),
    offset: int = Query(default=0),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    # -----------------------------------
    # Filtros dinâmicos
    # -----------------------------------
    filtros = ["h.tenant_id = %s"]
    where_params = [tenant_id]

    if data_inicio:
        filtros.append("DATE(h.criado_em) >= %s")
        where_params.append(data_inicio)   # <<< FALTAVA ISSO

    if data_fim:
        filtros.append("DATE(h.criado_em) <= %s")
        where_params.append(data_fim)       # <<< FALTAVA ISSO

    if descricao:
        filtros.append("LOWER(h.descricao) LIKE %s")
        where_params.append(f"%{descricao.lower()}%")



    where_clause = " AND ".join(filtros)

    conn = get_connection()

    # TOTAL REAL
    sql_total = f"""
        SELECT COUNT(*) 
        FROM historico_pipeline_jobs
        WHERE {where_clause};
    """
    total = pd.read_sql_query(
        sql_total,
        conn,
        params=where_params
    ).iloc[0, 0]


    # DADOS PAGINADOS
    sql = f"""
        SELECT
            job_id,
            criado_em,
            status,
            metadata->>'uf' AS uf,
            metadata->>'cidade' AS cidade,
            metadata->>'algo' AS algo,
            metadata->>'input_id' AS input_id,
            metadata->>'clusterization_id' AS clusterization_id,
            metadata->>'descricao' AS descricao
        FROM historico_pipeline_jobs
        WHERE {where_clause}
        ORDER BY criado_em DESC
        LIMIT %s OFFSET %s;
    """

    df = pd.read_sql_query(
        sql,
        conn,
        params=(*where_params, limit, offset),
    )


    conn.close()

    df = df.astype(object).replace({np.nan: None})

    return {
        "total": int(total),
        "jobs": df.to_dict(orient="records"),
    }


@router.get("/historico", dependencies=[Depends(verify_token)], tags=["Clusterização"])
def listar_historico_clusterizacao(
    request: Request,
    data_inicio: str | None = Query(None),
    data_fim: str | None = Query(None),
    descricao: str | None = Query(None),
    limit: int = Query(5, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    # -----------------------------------
    # Filtros dinâmicos
    # -----------------------------------
    filtros = ["h.tenant_id = %s"]
    where_params = [tenant_id]

    if data_inicio:
        filtros.append("DATE(h.criado_em) >= %s")
        where_params.append(data_inicio)

    if data_fim:
        filtros.append("DATE(h.criado_em) <= %s")
        where_params.append(data_fim)

    if descricao:
        filtros.append("LOWER(h.descricao) LIKE %s")
        where_params.append(f"%{descricao.lower()}%")


    where_clause = " AND ".join(filtros)

    conn = get_connection()

    sql = f"""
        WITH last_run AS (
            SELECT DISTINCT ON (clusterization_id)
                id AS run_id,
                clusterization_id
            FROM cluster_run
            WHERE tenant_id = %s
            ORDER BY clusterization_id, criado_em DESC
        ),
        resumo AS (
            SELECT
                lr.clusterization_id,
                COUNT(cs.id)::int                AS qtd_clusters,
                COALESCE(SUM(cs.n_pdvs), 0)::int AS pdvs_total
            FROM last_run lr
            JOIN cluster_setor cs
              ON cs.run_id = lr.run_id
            GROUP BY lr.clusterization_id
        )
        SELECT
            h.criado_em,
            h.clusterization_id,
            h.descricao,
            h.status,
            h.uf,
            h.cidade,
            h.algo,
            h.duracao_segundos,
            COALESCE(r.qtd_clusters, 0) AS qtd_clusters,
            COALESCE(r.pdvs_total, 0)   AS pdvs_total,
            COUNT(*) OVER()             AS total_registros
        FROM historico_cluster_jobs h
        LEFT JOIN resumo r
          ON r.clusterization_id = h.clusterization_id::uuid
        WHERE {where_clause}
        ORDER BY h.criado_em DESC
        LIMIT %s OFFSET %s;
    """

    df = pd.read_sql_query(
        sql,
        conn,
        params=[
            tenant_id,        # last_run
            *where_params,    # historico_cluster_jobs + filtros
            limit,
            offset,
        ],
    )

    conn.close()

    df = df.astype(object).replace({np.nan: None})

    total = int(df["total_registros"].iloc[0]) if len(df) else 0

    return {
        "total": total,
        "clusterizacoes": df.drop(columns=["total_registros"]).to_dict(orient="records"),
    }

# ============================================================
# 🔍 Detalhar job
# ============================================================
@router.get("/jobs/{job_id}", dependencies=[Depends(verify_token)], tags=["Clusterização"])
def detalhar_job(job_id: str):
    try:
        conn = Redis(host="redis", port=6379)
        job = Job.fetch(job_id, connection=conn)
        return {"job_id": job.id, "status": job.get_status(), "meta": job.meta, "params": job.args}
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
        return {"job_id": job.id, "status": job.get_status(), "meta": job.meta}
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
            "url_relativa": f"/output/maps/{tenant_id}/clusterization_{clusterization_id}.html",
        }

    except Exception as e:
        raise HTTPException(500, str(e))

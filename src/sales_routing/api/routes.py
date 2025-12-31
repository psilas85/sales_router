#sales_router/src/sales_routing/api/routes.py

# ============================================================
# 📦 sales_routing/api/routes.py — Rota REAL de roteirização (PADRÃO USE_CASES)
# ============================================================

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from sales_routing.api.dependencies import verify_token
from pydantic import BaseModel
import uuid
import os
import redis
from rq import Queue
from loguru import logger

from src.jobs.utils.job_status import registrar_job_status
from src.jobs.tasks.routing_task_parallel import executar_routing_master_job

router = APIRouter()

# ============================================================
# 📌 Dados enviados pelo frontend
# ============================================================
class RoteirizacaoRequest(BaseModel):
    clusterization_id: str
    descricao: str

    uf: str
    cidade: str | None = None

    dias_uteis: int
    frequencia_visita: int

    service_min: float
    vel_kmh: float

    # opcionais — defaults aplicados pelo backend
    alpha_path: float | None = None
    twoopt: bool | None = None
    modo_calculo: str | None = None


# ============================================================
# 🧪 Health check
# ============================================================
@router.get("/health")
async def health_check():
    return {"status": "ok", "service": "sales_routing"}


# ============================================================
# 📦 GET /routing/clusterizations
# Retorna clusterizações FINALIZADAS
# PADRÃO CANÔNICO (igual histórico de clusterização)
# ============================================================
@router.get("/clusterizations", dependencies=[Depends(verify_token)])
def listar_clusterizacoes_finalizadas(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    conn = get_connection()

    sql = """
        WITH last_run AS (
            SELECT DISTINCT ON (clusterization_id)
                id AS run_id,
                clusterization_id,
                descricao,
                uf,
                cidade,
                criado_em,
                status
            FROM cluster_run
            WHERE tenant_id = %s
            AND status = 'done'
            ORDER BY clusterization_id, criado_em DESC
        ),
        resumo AS (
            SELECT
                lr.clusterization_id,
                COUNT(cs.id)::int                AS qtd_clusters,
                COALESCE(SUM(cs.n_pdvs), 0)::int AS pdvs_total
            FROM last_run lr
            LEFT JOIN cluster_setor cs
            ON cs.run_id = lr.run_id
            GROUP BY lr.clusterization_id
        )
        SELECT
            lr.clusterization_id,
            lr.descricao,
            lr.uf,
            lr.cidade,
            lr.criado_em,
            r.qtd_clusters,
            r.pdvs_total,
            lr.status
        FROM last_run lr
        LEFT JOIN resumo r
        ON r.clusterization_id = lr.clusterization_id
        ORDER BY lr.criado_em DESC
        LIMIT 50;

    """

    df = pd.read_sql_query(sql, conn, params=(tenant_id,))
    conn.close()

    df = df.astype(object).replace({np.nan: None})
    return df.to_dict(orient="records")


# ============================================================
# 📦 GET /routing/runs — últimas execuções de roteirização (CORRETO)
# ============================================================
@router.get("/runs", dependencies=[Depends(verify_token)])
def listar_roteirizacoes_finalizadas(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    conn = get_connection()

    sql = """
        SELECT 
            r.routing_id,

            COALESCE(h.descricao, '') AS descricao,

            -- DATA CORRETA: ÚLTIMA execução do routing
            MAX(r.criado_em) AS criado_em,

            COUNT(DISTINCT r.cluster_id) AS clusters_processados,
            SUM(r.qtd_subclusters)       AS total_rotas,
            SUM(r.qtd_pdvs)              AS total_pdvs,
            SUM(r.dist_total_km)         AS total_km,
            SUM(r.tempo_total_min)       AS total_min

        FROM sales_routing_resumo r

        LEFT JOIN vw_historico_routing_jobs h
            ON h.routing_id = r.routing_id
            AND h.tenant_id = r.tenant_id

        WHERE r.tenant_id = %s

        GROUP BY r.routing_id, h.descricao

        ORDER BY MAX(r.criado_em) DESC

        LIMIT 200;
    """

    df = pd.read_sql_query(sql, conn, params=(tenant_id,))
    conn.close()

    df = df.astype(object).replace({np.nan: None})
    return df.to_dict(orient="records")


# ============================================================
# 📊 Status do Job de Roteirização (Redis + Fallback PGSQL)
# ============================================================
@router.get("/status/{job_id}", dependencies=[Depends(verify_token)])
def status_job(job_id: str):
    import redis
    import os
    import json
    from rq.job import Job

    # ----------------------------
    # Redis connection
    # ----------------------------
    redis_url = (
        os.getenv("REDIS_URL")
        or f"redis://{os.getenv('REDIS_HOST','redis')}:{os.getenv('REDIS_PORT','6379')}/0"
    )
    r = redis.from_url(redis_url)

    # ----------------------------
    # 1️⃣ Tentar ler status RQ Job.meta
    # ----------------------------
    try:
        job = Job.fetch(job_id, connection=r)

        progress = job.meta.get("progress", 0)
        status = job.meta.get("status", "running")
        mensagem = job.meta.get("mensagem", "")

        return {
            "progress": progress,
            "status": status,
            "message": mensagem,
        }

    except Exception:
        pass  # Continua para fallback no PostgreSQL

    # ----------------------------
    # 2️⃣ Fallback – buscar no historico_pipeline_jobs
    # ----------------------------
    try:
        from database.db_connection import get_connection
        conn = get_connection()
        cur = conn.cursor()

        cur.execute("""
            SELECT status, mensagem
            FROM historico_pipeline_jobs
            WHERE job_id = %s
            ORDER BY atualizado_em DESC
            LIMIT 1;
        """, (job_id,))

        row = cur.fetchone()
        cur.close()
        conn.close()

        if row:
            status_pg, mensagem_pg = row
            return {
                "progress": 100 if status_pg == "done" else 0,
                "status": status_pg,
                "message": mensagem_pg or "",
            }

    except Exception:
        pass

    # ----------------------------
    # 3️⃣ Caso não exista em nenhum lugar
    # ----------------------------
    return {
        "progress": 0,
        "status": "not_found",
        "message": "Job não encontrado"
    }

# ============================================================
# 🗺️ POST /routing/mapa — gera o mapa das rotas last-mile
# ============================================================
@router.post("/mapa", dependencies=[Depends(verify_token)])
def gerar_mapa_roteirizacao(
    request: Request,
    routing_id: str = Query(...),
):
    from sales_routing.visualization.route_plotting import (
        buscar_rotas_operacionais,
        gerar_mapa_rotas
    )
    from pathlib import Path

    user = request.state.user
    tenant_id = user["tenant_id"]

    dados = buscar_rotas_operacionais(tenant_id, routing_id)
    if not dados:
        raise HTTPException(404, "Nenhuma rota encontrada para routing_id informado.")

    output_dir = Path(f"output/maps/{tenant_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    output_path = output_dir / f"routing_{routing_id}.html"

    gerar_mapa_rotas(dados, output_path, modo_debug=False, zoom=9)

    return {
        "status": "success",
        "arquivo_html": str(output_path),
        "url_relativa": f"/output/maps/{tenant_id}/routing_{routing_id}.html"
    }

# ============================================================
# 📄 POST /routing/relatorio/resumo — exporta CSV resumido
# ============================================================
@router.post("/relatorio/resumo", dependencies=[Depends(verify_token)])
def routing_relatorio_resumo(request: Request, routing_id: str = Query(...)):

    from sales_routing.reporting.export_cluster_summary import exportar_resumo_cluster

    user = request.state.user
    tenant_id = user["tenant_id"]

    caminho = exportar_resumo_cluster(tenant_id, routing_id)
    if not caminho:
        raise HTTPException(404, "Nenhum registro encontrado para exportação.")

    # retorna caminho relativo para o frontend abrir via URL
    relativo = caminho.replace("/app/", "")
    return {"arquivo": relativo}


# ============================================================
# 📄 POST /routing/relatorio/pdvs — exporta XLSX detalhado
# ============================================================
@router.post("/relatorio/pdvs", dependencies=[Depends(verify_token)])
def routing_relatorio_pdvs(request: Request, routing_id: str = Query(...)):

    from sales_routing.reporting.export_pdvs_por_cluster import exportar_pdvs_por_cluster

    user = request.state.user
    tenant_id = user["tenant_id"]

    caminho = exportar_pdvs_por_cluster(tenant_id, routing_id)
    if not caminho:
        raise HTTPException(404, "Nenhum PDV encontrado para exportação.")

    relativo = str(caminho).replace("/app/", "")
    return {"arquivo": relativo}


# ============================================================
# 🚀 POST /routing/roteirizar
# ============================================================
@router.post("/roteirizar", dependencies=[Depends(verify_token)])
async def iniciar_roteirizacao(request: Request, body: RoteirizacaoRequest):

    # ============================================================
    # 🔐 SEMPRE pegar tenant_id do token (NUNCA do frontend!)
    # ============================================================
    user = request.state.user
    tenant_id = user["tenant_id"]

    # ============================================================
    # 🧾 Validações
    # ============================================================
    try:
        uuid.UUID(body.clusterization_id)
    except:
        raise HTTPException(status_code=400, detail="clusterization_id inválido")

    descricao = body.descricao.strip()
    if not descricao:
        raise HTTPException(status_code=400, detail="Descrição é obrigatória")
    descricao = descricao[:60]

    # ============================================================
    # 🆔 routing_id
    # ============================================================
    routing_id = str(uuid.uuid4())
    job_id = f"routing-master-{routing_id}"

    logger.info(f"🆕 Nova roteirização: routing_id={routing_id}")
    logger.info(f"tenant={tenant_id}, clusterization_id={body.clusterization_id}")
    logger.info(f"uf={body.uf}, cidade={body.cidade}")

    # ============================================================
    # ✔ Defaults oficiais
    # ============================================================
    alpha = body.alpha_path if body.alpha_path is not None else 1.3
    twoopt = body.twoopt if body.twoopt is not None else False
    modo_calculo = body.modo_calculo if body.modo_calculo is not None else "fixo"

    logger.info(
        f"Parâmetros: v_kmh={body.vel_kmh}, α={alpha}, "
        f"service_min={body.service_min}, modo_calculo={modo_calculo}, twoopt={twoopt}"
    )

    # ============================================================
    # 🔧 Redis
    # ============================================================
    redis_url = (
        os.getenv("REDIS_URL")
        or f"redis://{os.getenv('REDIS_HOST','redis')}:{os.getenv('REDIS_PORT','6379')}/0"
    )
    redis_conn = redis.from_url(redis_url)
    queue = Queue("routing_jobs", connection=redis_conn)

    # ============================================================
    # 📦 Parâmetros padronizados do job
    # ============================================================
    job_params = {
        "tenant_id": tenant_id,
        "routing_id": routing_id,
        "clusterization_id": body.clusterization_id,
        "descricao": descricao,

        "uf": body.uf,
        "cidade": body.cidade,

        "dias_uteis": body.dias_uteis,
        "frequencia_visita": body.frequencia_visita,

        "v_kmh": body.vel_kmh,
        "service_min": body.service_min,
        "alpha_path": alpha,

        "twoopt": twoopt,

        "modo": "fixo",
        "modo_calculo": modo_calculo,

        "usuario": user["email"],
    }

    # ============================================================
    # 🧾 Registrar status inicial
    # ============================================================
    registrar_job_status(
        job_id=job_id,
        tenant_id=tenant_id,
        etapa="routing",
        status="queued",
        mensagem=f"Job de roteirização enfileirado ({body.uf} - {body.cidade})",
        metadata=job_params,
    )

    # ============================================================
    # 🚀 Enfileirar job master
    # ============================================================
    job = queue.enqueue(
        executar_routing_master_job,
        job_params,
        job_timeout=int(os.getenv("ROUTING_JOB_TIMEOUT", 7200)),
        result_ttl=86400,
        failure_ttl=86400,
        job_id=job_id,
    )

    # ============================================================
    # 🔧 Inicializar META do job no Redis (ESSENCIAL!)
    # ============================================================
    job.meta["progress"] = 0
    job.meta["status"] = "queued"
    job.meta["mensagem"] = "Job enfileirado e aguardando execução"
    job.save_meta()

    logger.success(f"📤 routing job enfileirado: {job.id}")


    return {
        "status": "queued",
        "routing_id": routing_id,
        "job_id": job.id,
        "mensagem": "Roteirização iniciada com sucesso"
    }


# ============================================================
# 📦 GET /routing/relatorios — histórico consolidado de roteirizações
# (PADRÃO RELATÓRIOS – igual Setorização)
# ============================================================
@router.get("/relatorios", dependencies=[Depends(verify_token)])
def listar_relatorios_roteirizacao(
    request: Request,
    data_inicio: str | None = Query(None),
    data_fim: str | None = Query(None),
    descricao: str | None = Query(None),
    limit: int = Query(10),
    offset: int = Query(0),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    filtros = ["r.tenant_id = %s"]
    params = [tenant_id]

    if data_inicio and data_inicio.strip():
        filtros.append("DATE(r.criado_em) >= %s")
        params.append(data_inicio)

    if data_fim and data_fim.strip():
        filtros.append("DATE(r.criado_em) <= %s")
        params.append(data_fim)

    if descricao and descricao.strip():
        filtros.append("LOWER(h.descricao) LIKE %s")
        params.append(f"%{descricao.lower()}%")


    where_sql = " AND ".join(filtros)

    conn = get_connection()

    sql = f"""
        SELECT
            r.routing_id,
            COALESCE(h.descricao, '') AS descricao,
            MAX(r.criado_em) AS criado_em,
            COUNT(DISTINCT r.cluster_id) AS clusters,
            SUM(r.qtd_subclusters) AS total_rotas,
            SUM(r.qtd_pdvs) AS total_pdvs,
            SUM(r.dist_total_km) AS total_km,
            SUM(r.tempo_total_min) AS total_min
        FROM sales_routing_resumo r
        LEFT JOIN vw_historico_routing_jobs h
            ON h.routing_id = r.routing_id
            AND h.tenant_id = r.tenant_id
        WHERE {where_sql}
        GROUP BY r.routing_id, h.descricao
        ORDER BY MAX(r.criado_em) DESC
        LIMIT %s OFFSET %s;
    """

    sql_total = f"""
        SELECT COUNT(DISTINCT r.routing_id)
        FROM sales_routing_resumo r
        LEFT JOIN vw_historico_routing_jobs h
            ON h.routing_id = r.routing_id
            AND h.tenant_id = r.tenant_id
        WHERE {where_sql};
    """

    df = pd.read_sql_query(sql, conn, params=params + [limit, offset])
    total = pd.read_sql_query(sql_total, conn, params=params).iloc[0, 0]

    conn.close()

    df = df.astype(object).replace({np.nan: None})

    return {
        "roteirizacoes": df.to_dict(orient="records"),
        "total": int(total),
    }

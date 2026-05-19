#sales_router/src/sales_routing/api/routes.py

# ============================================================
# 📦 sales_routing/api/routes.py — Rota REAL de roteirização (PADRÃO USE_CASES)
# ============================================================

from fastapi import APIRouter, Depends, HTTPException, Request, Query
from fastapi.responses import StreamingResponse
from sales_routing.api.dependencies import verify_token
from pydantic import BaseModel
import io
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

    min_pdvs_rota: int
    max_pdvs_rota: int

    service_min: float

    # ------------------------------------------------------------
    # Novos campos (Etapa A — schema só; solver vem na Etapa B).
    # Optional para não quebrar clients antigos. Defaults espelham
    # ROUTING_DEFAULTS do frontend.
    # ------------------------------------------------------------
    # "heuristico" (default) ou "time_windows"
    algoritmo_roteirizacao: str = "heuristico"
    # Janelas (só usadas quando algoritmo_roteirizacao = "time_windows")
    horario_inicio_operacao: int = 480   # 08:00 em min desde 0h
    horario_fim_operacao: int = 1080     # 18:00
    usar_janelas_pdv: bool = False
    modo_estrito_janelas: bool = False
    # Janela padrão aplicada aos PDVs SEM janela individual, quando
    # usar_janelas_pdv=True e modo_estrito_janelas=False.
    janela_padrao_pdv_inicio: int = 480   # 08:00
    janela_padrao_pdv_fim: int = 1080     # 18:00
    max_estrategicos_por_rota: int = 1
    tempo_atendimento_especial_min: float = 90.0
    # Quando True (default), o solver faz uma 3ª passagem com janelas
    # abertas pra absorver PDVs que dropariam por inviabilidade local —
    # essas rotas são marcadas status_rota='fallback_excedente'.
    # Quando False, drops ficam fora (UI mostra como "não atendidos").
    permitir_rotas_excedentes: bool = True
    # Tempo máximo permitido por rota (minutos). 0 = sem limite extra
    # (usa só a janela operacional). Quando > 0, cada veículo é capado
    # em min(tempo_max_rota_min, janela_total). Rotas que ultrapassem
    # são marcadas como fallback_excedente.
    tempo_max_rota_min: int = 0


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
def listar_clusterizacoes_finalizadas(
    request: Request,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    descricao: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    """Lista clusterizações finalizadas com filtros + paginação server-side.

    Filtros (todos opcionais):
      - data_inicio / data_fim: ISO 'YYYY-MM-DD' (faixa inclusiva)
      - descricao: ILIKE %descricao% no campo `descricao`
      - limit/offset: paginação (default 50/0)

    Retorna {"items": [...], "total": <int>}.
    """
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    # ----- Monta filtros dinâmicos -----
    filtros = ["tenant_id = %s", "status = 'done'"]
    args: list = [tenant_id]
    # `criado_em` é TIMESTAMPTZ (UTC); filtros do front vêm no fuso BRT.
    # Interpretamos a data como 00:00 / 24:00 em America/Sao_Paulo.
    if data_inicio:
        filtros.append("criado_em >= (%s::timestamp AT TIME ZONE 'America/Sao_Paulo')")
        args.append(data_inicio)
    if data_fim:
        filtros.append("criado_em <  ((%s::date + 1)::timestamp AT TIME ZONE 'America/Sao_Paulo')")
        args.append(data_fim)
    if descricao and descricao.strip():
        filtros.append("descricao ILIKE %s")
        args.append(f"%{descricao.strip()}%")
    where_clause = " AND ".join(filtros)

    # ----- Bound seguro pra paginação -----
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))

    conn = get_connection()
    try:
        # Total de clusterizations distintas que satisfazem os filtros
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(DISTINCT clusterization_id)
                FROM cluster_run
                WHERE {where_clause}
                """,
                tuple(args),
            )
            total = int(cur.fetchone()[0] or 0)

        # Items paginados
        items_sql = f"""
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
                WHERE {where_clause}
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
            LIMIT %s OFFSET %s
        """
        df = pd.read_sql_query(items_sql, conn, params=tuple(args + [limit, offset]))
    finally:
        conn.close()

    df = df.astype(object).replace({np.nan: None})
    return {"items": df.to_dict(orient="records"), "total": total}


# ============================================================
# 📦 GET /routing/runs — últimas execuções de roteirização (CORRETO)
# ============================================================
@router.get("/runs", dependencies=[Depends(verify_token)])
def listar_roteirizacoes_finalizadas(
    request: Request,
    data_inicio: str | None = None,
    data_fim: str | None = None,
    descricao: str | None = None,
    limit: int = 50,
    offset: int = 0,
):
    """Lista roteirizações finalizadas com filtros + paginação server-side.

    Filtros (todos opcionais):
      - data_inicio / data_fim: ISO 'YYYY-MM-DD' (faixa inclusiva, aplicada
        sobre MAX(criado_em) de cada routing_id no `sales_routing_resumo`)
      - descricao: ILIKE %descricao% no campo `descricao` do histórico
      - limit/offset: paginação (default 50/0)

    Retorna {"items": [...], "total": <int>}.
    """
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection
    import pandas as pd
    import numpy as np

    # ----- Filtros HAVING (aplicam após agregação) -----
    having_parts: list[str] = []
    having_args: list = []
    # `criado_em` é TIMESTAMPTZ (UTC); filtros do front vêm no fuso BRT.
    if data_inicio:
        having_parts.append(
            "MAX(r.criado_em) >= (%s::timestamp AT TIME ZONE 'America/Sao_Paulo')"
        )
        having_args.append(data_inicio)
    if data_fim:
        having_parts.append(
            "MAX(r.criado_em) <  ((%s::date + 1)::timestamp AT TIME ZONE 'America/Sao_Paulo')"
        )
        having_args.append(data_fim)

    # Descricao filtra antes — pode usar WHERE no JOIN
    where_extra = ""
    where_extra_args: list = []
    if descricao and descricao.strip():
        where_extra = " AND h.descricao ILIKE %s"
        where_extra_args.append(f"%{descricao.strip()}%")

    having_clause = ("HAVING " + " AND ".join(having_parts)) if having_parts else ""

    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))

    base_args = [tenant_id] + where_extra_args
    items_args = tuple(base_args + having_args + [limit, offset])
    count_args = tuple(base_args + having_args)

    conn = get_connection()
    try:
        # Total: COUNT sobre a mesma agregação
        count_sql = f"""
            SELECT COUNT(*) FROM (
                SELECT r.routing_id
                FROM sales_routing_resumo r
                LEFT JOIN vw_historico_routing_jobs h
                    ON h.routing_id = r.routing_id
                    AND h.tenant_id = r.tenant_id
                WHERE r.tenant_id = %s {where_extra}
                GROUP BY r.routing_id, h.descricao
                {having_clause}
            ) AS sub
        """
        with conn.cursor() as cur:
            cur.execute(count_sql, count_args)
            total = int(cur.fetchone()[0] or 0)

        items_sql = f"""
            SELECT
                r.routing_id,
                COALESCE(h.descricao, '') AS descricao,
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
            WHERE r.tenant_id = %s {where_extra}
            GROUP BY r.routing_id, h.descricao
            {having_clause}
            ORDER BY MAX(r.criado_em) DESC
            LIMIT %s OFFSET %s
        """
        df = pd.read_sql_query(items_sql, conn, params=items_args)
    finally:
        conn.close()

    df = df.astype(object).replace({np.nan: None})
    return {"items": df.to_dict(orient="records"), "total": total}


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
# Endpoints POST /routing/mapa e /routing/mapa_async removidos em 2026-05-18:
# geravam HTML Folium em /app/output/maps/ (1 arquivo por roteirização, sem
# cleanup) e o /mapa_async ainda estava quebrado (NameError em `queue`).
# A UI nova vai usar GET /routing/{routing_id}/pontos + Leaflet inline,
# seguindo o padrão DB-centric que aplicamos em setorização.

# ============================================================
# 📄 Exports XLSX — agora em memória (StreamingResponse, sem persistir em disco)
# ============================================================
def _xlsx_response(payload: bytes, filename: str) -> StreamingResponse:
    return StreamingResponse(
        io.BytesIO(payload),
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# Aceita GET e POST por compatibilidade — frontend antigo usava POST,
# mas GET facilita download direto (responseType: blob).
@router.api_route(
    "/relatorio/resumo",
    methods=["GET", "POST"],
    dependencies=[Depends(verify_token)],
)
def routing_relatorio_resumo(request: Request, routing_id: str = Query(...)):
    from sales_routing.reporting.export_cluster_summary import (
        routing_resumo_to_bytes,
    )

    user = request.state.user
    tenant_id = user["tenant_id"]
    try:
        data = routing_resumo_to_bytes(tenant_id, routing_id)
        return _xlsx_response(data, f"routing_resumo_{routing_id}.xlsx")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.api_route(
    "/relatorio/pdvs",
    methods=["GET", "POST"],
    dependencies=[Depends(verify_token)],
)
def routing_relatorio_pdvs(request: Request, routing_id: str = Query(...)):
    from sales_routing.reporting.export_pdvs_por_cluster import (
        routing_pdvs_to_bytes,
    )

    user = request.state.user
    tenant_id = user["tenant_id"]
    try:
        data = routing_pdvs_to_bytes(tenant_id, routing_id)
        return _xlsx_response(data, f"pdvs_por_cluster_{routing_id}.xlsx")
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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

    logger.info(
        f"Parâmetros: dias_uteis={body.dias_uteis}, "
        f"freq={body.frequencia_visita}, "
        f"min_pdvs={body.min_pdvs_rota}, "
        f"max_pdvs={body.max_pdvs_rota}, "
        f"service_min={body.service_min}"
    )

    if body.min_pdvs_rota > body.max_pdvs_rota:
        raise HTTPException(
            status_code=400,
            detail="min_pdvs_rota não pode ser maior que max_pdvs_rota"
        )

    if body.frequencia_visita <= 0:
        raise HTTPException(status_code=400, detail="frequencia_visita deve ser > 0")

    if body.dias_uteis <= 0:
        raise HTTPException(status_code=400, detail="dias_uteis deve ser > 0")

    if body.algoritmo_roteirizacao not in ("heuristico", "time_windows"):
        raise HTTPException(
            status_code=400,
            detail="algoritmo_roteirizacao deve ser 'heuristico' ou 'time_windows'",
        )

    if body.algoritmo_roteirizacao == "time_windows":
        if not (0 <= body.horario_inicio_operacao < 1440):
            raise HTTPException(
                status_code=400, detail="horario_inicio_operacao fora de [0, 1440)"
            )
        if not (0 <= body.horario_fim_operacao < 1440):
            raise HTTPException(
                status_code=400, detail="horario_fim_operacao fora de [0, 1440)"
            )
        if body.horario_fim_operacao <= body.horario_inicio_operacao:
            raise HTTPException(
                status_code=400,
                detail="horario_fim_operacao deve ser maior que horario_inicio_operacao",
            )
        # Janela padrão só importa quando usa janelas individuais SEM modo
        # estrito; ainda assim validamos as faixas defensivamente.
        if not (0 <= body.janela_padrao_pdv_inicio < 1440):
            raise HTTPException(
                status_code=400, detail="janela_padrao_pdv_inicio fora de [0, 1440)"
            )
        if not (0 <= body.janela_padrao_pdv_fim < 1440):
            raise HTTPException(
                status_code=400, detail="janela_padrao_pdv_fim fora de [0, 1440)"
            )
        if (
            body.usar_janelas_pdv
            and not body.modo_estrito_janelas
            and body.janela_padrao_pdv_fim <= body.janela_padrao_pdv_inicio
        ):
            raise HTTPException(
                status_code=400,
                detail="janela_padrao_pdv_fim deve ser maior que janela_padrao_pdv_inicio",
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

        "min_pdvs_rota": body.min_pdvs_rota,
        "max_pdvs_rota": body.max_pdvs_rota,

        "service_min": body.service_min,

        "v_kmh": 35,
        "alpha_path": 1.3,

        "modo": "balanceado",
        "modo_calculo": "frequencia",

        "twoopt": False,

        # ---- Etapa A: schema só. Worker registra mas ainda não roda
        # CVRPTW — quando algoritmo_roteirizacao == "time_windows", o
        # worker cai no heurístico com aviso (até a Etapa B).
        "algoritmo_roteirizacao": body.algoritmo_roteirizacao,
        "horario_inicio_operacao": body.horario_inicio_operacao,
        "horario_fim_operacao": body.horario_fim_operacao,
        "usar_janelas_pdv": body.usar_janelas_pdv,
        "modo_estrito_janelas": body.modo_estrito_janelas,
        "janela_padrao_pdv_inicio": body.janela_padrao_pdv_inicio,
        "janela_padrao_pdv_fim": body.janela_padrao_pdv_fim,
        "max_estrategicos_por_rota": body.max_estrategicos_por_rota,
        "tempo_atendimento_especial_min": body.tempo_atendimento_especial_min,
        "permitir_rotas_excedentes": body.permitir_rotas_excedentes,
        "tempo_max_rota_min": body.tempo_max_rota_min,

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

    # `criado_em` é TIMESTAMPTZ (UTC); filtros do front vêm em BRT.
    if data_inicio and data_inicio.strip():
        filtros.append(
            "r.criado_em >= (%s::timestamp AT TIME ZONE 'America/Sao_Paulo')"
        )
        params.append(data_inicio)

    if data_fim and data_fim.strip():
        filtros.append(
            "r.criado_em <  ((%s::date + 1)::timestamp AT TIME ZONE 'America/Sao_Paulo')"
        )
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

# /mapa_async removido (vide comentário no topo do bloco antigo de /mapa).


# ============================================================
# 📍 Pontos + rotas de uma roteirização (mapa Leaflet inline no frontend)
# ============================================================
# Substitui o HTML Folium. Frontend renderiza pontos coloridos por
# subcluster (rota diária) + polylines da sequência de visita.
import math


def _f(v):
    """NaN/Inf seguros pra JSON encoder do FastAPI."""
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


@router.get("/{routing_id}/pontos", dependencies=[Depends(verify_token)])
def listar_pontos_roteirizacao(
    request: Request,
    routing_id: str,
    limit: int = Query(default=5000, le=20000),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from src.database.db_connection import get_connection

    conn = get_connection()
    try:
        cur = conn.cursor()

        # Subclusters (rotas diárias) com metadados — usado como
        # "setor" no Leaflet (1 cor por subcluster).
        cur.execute(
            """
            SELECT
                id, cluster_id, subcluster_seq, n_pdvs,
                tempo_total_min, dist_total_km,
                centro_lat, centro_lon, rota_coord,
                tempo_parcial_min, dist_parcial_km,
                status_rota, timeline_eventos, horario_inicio_operacao
            FROM sales_subcluster
            WHERE tenant_id = %s AND routing_id = %s
            ORDER BY cluster_id, subcluster_seq;
            """,
            (tenant_id, routing_id),
        )
        subclusters = []
        for r in cur.fetchall():
            subclusters.append(
                {
                    "id": int(r[0]),
                    "cluster_id": int(r[1]) if r[1] is not None else None,
                    "subcluster_seq": int(r[2] or 0),
                    "n_pdvs": int(r[3] or 0),
                    "tempo_total_min": _f(r[4]) or 0.0,
                    "dist_total_km": _f(r[5]) or 0.0,
                    "centro_lat": _f(r[6]),
                    "centro_lon": _f(r[7]),
                    # rota_coord é jsonb (lista de pares [lat,lon]) usada
                    # pra desenhar Polyline no Leaflet.
                    "rota_coord": r[8] if r[8] else [],
                    # Parciais (até último PDV, sem retorno ao centro)
                    # — preenchido só nas roteirizações time_windows;
                    # null nas geradas por modos heurísticos antigos.
                    "tempo_parcial_min": _f(r[9]),
                    "dist_parcial_km": _f(r[10]),
                    "status_rota": r[11] if r[11] else None,
                    # Lista de eventos pro Gantt: [{tipo, inicio_min,
                    # fim_min, pdv_id?}]. Tempos relativos ao
                    # horario_inicio_operacao. Null em heurístico antigo.
                    "timeline_eventos": r[12] if r[12] else None,
                    "horario_inicio_operacao": int(r[13]) if r[13] is not None else None,
                }
            )

        if not subclusters:
            raise HTTPException(
                404, "Roteirização não encontrada para este tenant."
            )

        # Pontos individuais — payload enxuto pra popup do mapa
        cur.execute(
            """
            SELECT spp.lat, spp.lon, spp.cluster_id, spp.subcluster_seq,
                   spp.sequencia_ordem,
                   p.cnpj, p.cidade, p.uf, p.pdv_endereco_completo, p.pdv_vendas
            FROM sales_subcluster_pdv spp
            LEFT JOIN pdvs p ON p.id = spp.pdv_id
            WHERE spp.tenant_id = %s AND spp.routing_id = %s
              AND spp.lat IS NOT NULL AND spp.lon IS NOT NULL
            ORDER BY spp.cluster_id, spp.subcluster_seq, spp.sequencia_ordem
            LIMIT %s;
            """,
            (tenant_id, routing_id, int(limit)),
        )
        pontos = []
        for r in cur.fetchall():
            lat = _f(r[0])
            lon = _f(r[1])
            if lat is None or lon is None:
                continue
            pontos.append(
                {
                    "lat": lat,
                    "lon": lon,
                    "cluster_id": int(r[2]) if r[2] is not None else None,
                    "subcluster_seq": int(r[3] or 0),
                    "sequencia_ordem": int(r[4] or 0),
                    "cnpj": r[5],
                    "cidade": r[6],
                    "uf": r[7],
                    "endereco": r[8],
                    "pdv_vendas": _f(r[9]),
                }
            )

        # Centros dos setores originais (cluster_setor) — pra o mapa
        # destacar o "anchor" de cada setor (1 marker grande por setor)
        # além dos centros das rotas individuais (1 por subcluster).
        cur.execute(
            """
            SELECT DISTINCT cs.id, cs.cluster_label, cs.centro_lat, cs.centro_lon, cs.n_pdvs
            FROM sales_subcluster ss
            JOIN cluster_setor cs ON cs.id = ss.cluster_id
            WHERE ss.tenant_id = %s AND ss.routing_id = %s
            ORDER BY cs.id;
            """,
            (tenant_id, routing_id),
        )
        clusters = [
            {
                "id": int(r[0]),
                "cluster_label": int(r[1]),
                "centro_lat": _f(r[2]),
                "centro_lon": _f(r[3]),
                "n_pdvs": int(r[4] or 0),
            }
            for r in cur.fetchall()
        ]

        # Params da roteirização (frequencia_visita, dias_uteis etc) —
        # gravados em historico_subcluster_jobs.params. Pode não existir
        # em roteirizações antigas → fallback {}.
        params_routing: dict = {}
        try:
            cur.execute(
                """
                SELECT params FROM historico_subcluster_jobs
                WHERE tenant_id = %s AND routing_id = %s
                LIMIT 1;
                """,
                (tenant_id, routing_id),
            )
            row_params = cur.fetchone()
            if row_params and row_params[0]:
                params_routing = row_params[0] if isinstance(row_params[0], dict) else {}
        except Exception:
            # Coluna params pode não existir em ambientes antigos.
            params_routing = {}

        return {
            "routing_id": routing_id,
            "total_pontos": len(pontos),
            "clusters": clusters,
            "subclusters": subclusters,
            "pontos": pontos,
            "params": params_routing,
        }
    finally:
        conn.close()


# ============================================================
# 🎛️ Defaults de parâmetros do tenant (Roteirização)
# ============================================================
# Mesmo padrão de tenant_cluster_defaults: lazy migration via
# CREATE TABLE IF NOT EXISTS. Armazena por tenant_id os defaults
# de roteirização (dias_uteis, frequencia_visita, min/max_pdvs,
# service_min, vel_kmh).
def _ensure_tenant_routing_defaults_table():
    from src.database.db_connection import get_connection

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tenant_routing_defaults (
                tenant_id INTEGER PRIMARY KEY,
                params JSONB NOT NULL,
                atualizado_em TIMESTAMP NOT NULL DEFAULT NOW(),
                atualizado_por TEXT
            );
            """
        )
        conn.commit()
    finally:
        conn.close()


@router.get("/defaults", dependencies=[Depends(verify_token)])
def get_routing_defaults(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]

    _ensure_tenant_routing_defaults_table()

    from src.database.db_connection import get_connection

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT params, atualizado_em FROM tenant_routing_defaults WHERE tenant_id = %s",
            (tenant_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"params": None, "atualizado_em": None}
        return {
            "params": row[0],
            "atualizado_em": row[1].isoformat() if row[1] else None,
        }
    finally:
        conn.close()


@router.put("/defaults", dependencies=[Depends(verify_token)])
async def save_routing_defaults(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]
    email = user.get("email") or "?"

    body = await request.json()
    params = body.get("params")
    if not isinstance(params, dict):
        raise HTTPException(400, "Body deve conter 'params' (objeto).")

    _ensure_tenant_routing_defaults_table()

    from src.database.db_connection import get_connection
    import json

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tenant_routing_defaults (tenant_id, params, atualizado_em, atualizado_por)
            VALUES (%s, %s::jsonb, NOW(), %s)
            ON CONFLICT (tenant_id) DO UPDATE
            SET params = EXCLUDED.params,
                atualizado_em = NOW(),
                atualizado_por = EXCLUDED.atualizado_por;
            """,
            (tenant_id, json.dumps(params), email),
        )
        conn.commit()
        logger.info(f"🎛️ Routing defaults salvos | tenant={tenant_id} | por={email}")
        return {"status": "saved", "tenant_id": tenant_id}
    finally:
        conn.close()

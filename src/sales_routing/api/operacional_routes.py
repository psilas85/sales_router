# sales_router/src/sales_routing/api/operacional_routes.py
#
# Roteirização da EXECUÇÃO OPERACIONAL — endpoints sob /routing/operacional/*.
#
# Mesmo pipeline da Simulação (routing_task_parallel), porém com
# schema="operacional" no payload do job: lê os setores de operacional.*
# (saída da setorização operacional) e persiste em operacional.sales_*.
#
# Modo rápido (heurístico) → delega ao routing_engine (payload-based, sem
# tocar o banco). Modo janelas (time_windows) → CVRPTW local. As tabelas
# globais (consultores, route_cache, agenda*) seguem em `public`.

import math
import os
import uuid
from datetime import timezone

import redis
from fastapi import APIRouter, Depends, HTTPException, Query, Request
from pydantic import BaseModel
from rq import Queue
from loguru import logger

from sales_routing.api.dependencies import verify_token
from sales_routing.api.routes import (
    RoteirizacaoRequest,
    status_job,
    _xlsx_response,
)
from src.database.db_connection import get_connection_context
from src.jobs.tasks.routing_task_parallel import executar_routing_master_job
from src.jobs.utils.job_status import registrar_job_status

router = APIRouter()

_OP = "operacional"


def _iso_utc(dt):
    """ISO 8601 com offset UTC explícito. cluster_run.criado_em e
    sales_routing_resumo.criado_em são `timestamp` naive guardando UTC —
    marca como UTC ao serializar p/ o front converter ao fuso local."""
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


def _f(v):
    """NaN/Inf seguros para o JSON encoder do FastAPI."""
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _redis_queue() -> Queue:
    redis_url = (
        os.getenv("REDIS_URL")
        or f"redis://{os.getenv('REDIS_HOST', 'redis')}:"
        f"{os.getenv('REDIS_PORT', '6379')}/0"
    )
    return Queue("routing_jobs", connection=redis.from_url(redis_url))


# ============================================================
# 🩺 Health
# ============================================================
@router.get("/operacional/health")
async def health_check():
    return {"status": "ok", "service": "sales_routing", "schema": _OP}


# ============================================================
# 📦 Setorizações operacionais disponíveis para roteirização
# ============================================================
@router.get("/operacional/clusterizations", dependencies=[Depends(verify_token)])
def listar_clusterizacoes_operacional(
    request: Request,
    data_inicio: str | None = Query(None),
    data_fim: str | None = Query(None),
    descricao: str | None = Query(None),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Setorizações operacionais concluídas (operacional.cluster_run done)."""
    tenant_id = request.state.user["tenant_id"]
    busca = (
        f"%{descricao.strip()}%" if descricao and descricao.strip() else None
    )
    # operacional.cluster_run.criado_em é timestamp naive (UTC) — converte
    # ao fuso local antes de filtrar pela data escolhida pelo usuário.
    filtro = """
          AND (%s IS NULL OR
               (cr.criado_em AT TIME ZONE 'UTC'
                  AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date)
          AND (%s IS NULL OR
               (cr.criado_em AT TIME ZONE 'UTC'
                  AT TIME ZONE 'America/Sao_Paulo')::date <= %s::date)
          AND (%s IS NULL OR cr.descricao ILIKE %s)
    """
    args = (data_inicio, data_inicio, data_fim, data_fim, busca, busca)

    with get_connection_context(schema=_OP) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"SELECT COUNT(*) FROM cluster_run cr "
                f"WHERE cr.tenant_id = %s AND cr.status = 'done' {filtro};",
                (tenant_id, *args),
            )
            total = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                SELECT cr.clusterization_id, cr.descricao, cr.uf, cr.cidade,
                       cr.criado_em, cr.status,
                       COUNT(cs.id)::int                AS qtd_clusters,
                       COALESCE(SUM(cs.n_pdvs), 0)::int AS pdvs_total
                FROM cluster_run cr
                LEFT JOIN cluster_setor cs ON cs.run_id = cr.id
                WHERE cr.tenant_id = %s AND cr.status = 'done'
                {filtro}
                GROUP BY cr.id
                ORDER BY cr.criado_em DESC
                LIMIT %s OFFSET %s;
                """,
                (tenant_id, *args, limit, offset),
            )
            items = [
                {
                    "clusterization_id": str(r[0]),
                    "descricao": r[1],
                    "uf": r[2],
                    "cidade": r[3],
                    "criado_em": _iso_utc(r[4]),
                    "status": r[5],
                    "qtd_clusters": int(r[6] or 0),
                    "pdvs_total": int(r[7] or 0),
                }
                for r in cur.fetchall()
            ]
    return {"items": items, "total": total}


# ============================================================
# 🚀 Iniciar roteirização operacional
# ============================================================
@router.post("/operacional/roteirizar", dependencies=[Depends(verify_token)])
async def iniciar_roteirizacao_operacional(
    request: Request, body: RoteirizacaoRequest
):
    tenant_id = request.state.user["tenant_id"]
    user = request.state.user

    try:
        uuid.UUID(body.clusterization_id)
    except Exception:
        raise HTTPException(400, "clusterization_id inválido")

    descricao = (body.descricao or "").strip()[:60]
    if not descricao:
        raise HTTPException(400, "Descrição é obrigatória")
    if body.min_pdvs_rota > body.max_pdvs_rota:
        raise HTTPException(
            400, "min_pdvs_rota não pode ser maior que max_pdvs_rota"
        )
    if body.frequencia_visita <= 0:
        raise HTTPException(400, "frequencia_visita deve ser > 0")
    if body.dias_uteis <= 0:
        raise HTTPException(400, "dias_uteis deve ser > 0")
    if body.algoritmo_roteirizacao not in ("heuristico", "time_windows"):
        raise HTTPException(
            400, "algoritmo_roteirizacao deve ser 'heuristico' ou 'time_windows'"
        )
    if body.algoritmo_roteirizacao == "time_windows":
        if body.horario_fim_operacao <= body.horario_inicio_operacao:
            raise HTTPException(
                400,
                "horario_fim_operacao deve ser maior que horario_inicio_operacao",
            )

    routing_id = str(uuid.uuid4())
    job_id = f"routing-master-{routing_id}"

    # schema="operacional" é o que direciona todo o pipeline para o schema
    # operacional (leitura dos setores + persistência das rotas).
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
        "schema": _OP,
    }

    registrar_job_status(
        job_id=job_id,
        tenant_id=tenant_id,
        etapa="routing",
        status="queued",
        mensagem=f"Roteirização operacional enfileirada ({body.uf})",
        metadata=job_params,
    )

    job = _redis_queue().enqueue(
        executar_routing_master_job,
        job_params,
        job_timeout=int(os.getenv("ROUTING_JOB_TIMEOUT", 7200)),
        result_ttl=86400,
        failure_ttl=86400,
        job_id=job_id,
    )
    job.meta["progress"] = 0
    job.meta["status"] = "queued"
    job.meta["mensagem"] = "Job enfileirado e aguardando execução"
    job.save_meta()

    logger.success(f"📤 routing operacional enfileirado: {job.id}")
    return {
        "status": "queued",
        "routing_id": routing_id,
        "job_id": job.id,
        "mensagem": "Roteirização operacional iniciada com sucesso",
    }


# ============================================================
# 📊 Status do job — reaproveita o handler da Simulação
# (job tracking via Redis + historico_pipeline_jobs é schema-agnóstico)
# ============================================================
@router.get("/operacional/status/{job_id}", dependencies=[Depends(verify_token)])
def status_job_operacional(job_id: str):
    return status_job(job_id)


# ============================================================
# 📦 Histórico de roteirizações operacionais
# ============================================================
@router.get("/operacional/relatorios", dependencies=[Depends(verify_token)])
def listar_relatorios_operacional(
    request: Request,
    data_inicio: str | None = Query(None),
    data_fim: str | None = Query(None),
    descricao: str | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    tenant_id = request.state.user["tenant_id"]
    busca = (
        f"%{descricao.strip()}%" if descricao and descricao.strip() else None
    )
    # sales_routing_resumo.criado_em é timestamp naive (UTC).
    filtro = """
          AND (%s IS NULL OR
               (r.criado_em AT TIME ZONE 'UTC'
                  AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date)
          AND (%s IS NULL OR
               (r.criado_em AT TIME ZONE 'UTC'
                  AT TIME ZONE 'America/Sao_Paulo')::date <= %s::date)
          AND (%s IS NULL OR h.descricao ILIKE %s)
    """
    args = (data_inicio, data_inicio, data_fim, data_fim, busca, busca)

    with get_connection_context(schema=_OP) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT r.routing_id
                    FROM sales_routing_resumo r
                    LEFT JOIN historico_subcluster_jobs h
                      ON h.routing_id = r.routing_id
                     AND h.tenant_id = r.tenant_id
                    WHERE r.tenant_id = %s {filtro}
                    GROUP BY r.routing_id, h.descricao
                ) t;
                """,
                (tenant_id, *args),
            )
            total = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                SELECT r.routing_id,
                       COALESCE(h.descricao, '')       AS descricao,
                       MAX(r.criado_em)                AS criado_em,
                       COUNT(DISTINCT r.cluster_id)    AS clusters,
                       SUM(r.qtd_subclusters)          AS total_rotas,
                       SUM(r.qtd_pdvs)                 AS total_pdvs,
                       SUM(r.dist_total_km)            AS total_km,
                       SUM(r.tempo_total_min)          AS total_min
                FROM sales_routing_resumo r
                LEFT JOIN historico_subcluster_jobs h
                  ON h.routing_id = r.routing_id
                 AND h.tenant_id = r.tenant_id
                WHERE r.tenant_id = %s {filtro}
                GROUP BY r.routing_id, h.descricao
                ORDER BY MAX(r.criado_em) DESC
                LIMIT %s OFFSET %s;
                """,
                (tenant_id, *args, limit, offset),
            )
            roteirizacoes = [
                {
                    "routing_id": str(r[0]),
                    "descricao": r[1],
                    "criado_em": _iso_utc(r[2]),
                    "clusters": int(r[3] or 0),
                    "total_rotas": int(r[4] or 0),
                    "total_pdvs": int(r[5] or 0),
                    "total_km": _f(r[6]) or 0.0,
                    "total_min": _f(r[7]) or 0.0,
                }
                for r in cur.fetchall()
            ]
    return {"roteirizacoes": roteirizacoes, "total": total}


# ============================================================
# 📍 Pontos + rotas de uma roteirização operacional (mapa Leaflet)
# ============================================================
@router.get(
    "/operacional/{routing_id}/pontos", dependencies=[Depends(verify_token)]
)
def listar_pontos_operacional(
    request: Request,
    routing_id: str,
    limit: int = Query(default=5000, le=20000),
):
    tenant_id = request.state.user["tenant_id"]

    with get_connection_context(schema=_OP) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, cluster_id, subcluster_seq, n_pdvs,
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
            subclusters = [
                {
                    "id": int(r[0]),
                    "cluster_id": int(r[1]) if r[1] is not None else None,
                    "subcluster_seq": int(r[2] or 0),
                    "n_pdvs": int(r[3] or 0),
                    "tempo_total_min": _f(r[4]) or 0.0,
                    "dist_total_km": _f(r[5]) or 0.0,
                    "centro_lat": _f(r[6]),
                    "centro_lon": _f(r[7]),
                    "rota_coord": r[8] if r[8] else [],
                    "tempo_parcial_min": _f(r[9]),
                    "dist_parcial_km": _f(r[10]),
                    "status_rota": r[11] if r[11] else None,
                    "timeline_eventos": r[12] if r[12] else None,
                    "horario_inicio_operacao": (
                        int(r[13]) if r[13] is not None else None
                    ),
                }
                for r in cur.fetchall()
            ]
            if not subclusters:
                raise HTTPException(
                    404, "Roteirização não encontrada para este tenant."
                )

            cur.execute(
                """
                SELECT spp.lat, spp.lon, spp.cluster_id, spp.subcluster_seq,
                       spp.sequencia_ordem,
                       p.cnpj, p.cidade, p.uf, p.pdv_endereco_completo,
                       p.pdv_vendas
                FROM sales_subcluster_pdv spp
                LEFT JOIN pdvs p ON p.id = spp.pdv_id
                WHERE spp.tenant_id = %s AND spp.routing_id = %s
                  AND spp.lat IS NOT NULL AND spp.lon IS NOT NULL
                ORDER BY spp.cluster_id, spp.subcluster_seq,
                         spp.sequencia_ordem
                LIMIT %s;
                """,
                (tenant_id, routing_id, int(limit)),
            )
            pontos = []
            for r in cur.fetchall():
                lat, lon = _f(r[0]), _f(r[1])
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

            cur.execute(
                """
                SELECT DISTINCT cs.id, cs.cluster_label,
                       cs.centro_lat, cs.centro_lon, cs.n_pdvs
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

            params_routing: dict = {}
            try:
                cur.execute(
                    """
                    SELECT params FROM historico_subcluster_jobs
                    WHERE tenant_id = %s AND routing_id = %s LIMIT 1;
                    """,
                    (tenant_id, routing_id),
                )
                row = cur.fetchone()
                if row and row[0]:
                    params_routing = row[0] if isinstance(row[0], dict) else {}
            except Exception:
                params_routing = {}

    return {
        "routing_id": routing_id,
        "total_pontos": len(pontos),
        "clusters": clusters,
        "subclusters": subclusters,
        "pontos": pontos,
        "params": params_routing,
    }


# ============================================================
# 📄 Exports XLSX — resumo por cluster / PDVs por cluster (on-demand)
# ============================================================
@router.api_route(
    "/operacional/relatorio/resumo",
    methods=["GET", "POST"],
    dependencies=[Depends(verify_token)],
)
def routing_relatorio_resumo_operacional(
    request: Request, routing_id: str = Query(...)
):
    from sales_routing.reporting.export_cluster_summary import (
        routing_resumo_to_bytes,
    )

    tenant_id = request.state.user["tenant_id"]
    try:
        data = routing_resumo_to_bytes(tenant_id, routing_id, schema=_OP)
        return _xlsx_response(
            data, f"routing_operacional_resumo_{routing_id}.xlsx"
        )
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


@router.api_route(
    "/operacional/relatorio/pdvs",
    methods=["GET", "POST"],
    dependencies=[Depends(verify_token)],
)
def routing_relatorio_pdvs_operacional(
    request: Request, routing_id: str = Query(...)
):
    from sales_routing.reporting.export_pdvs_por_cluster import (
        routing_pdvs_to_bytes,
    )

    tenant_id = request.state.user["tenant_id"]
    try:
        data = routing_pdvs_to_bytes(tenant_id, routing_id, schema=_OP)
        return _xlsx_response(
            data, f"routing_operacional_pdvs_{routing_id}.xlsx"
        )
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))


# ============================================================
# 🎛️ Defaults de parâmetros de roteirização operacional (por tenant)
# ============================================================
def _ensure_routing_operacional_defaults_table() -> None:
    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS
                public.tenant_routing_operacional_defaults (
                    tenant_id INTEGER PRIMARY KEY,
                    params JSONB NOT NULL,
                    atualizado_em TIMESTAMP NOT NULL DEFAULT NOW(),
                    atualizado_por TEXT
                );
                """
            )
        conn.commit()


@router.get("/operacional/defaults", dependencies=[Depends(verify_token)])
def get_routing_defaults_operacional(request: Request):
    tenant_id = request.state.user["tenant_id"]
    _ensure_routing_operacional_defaults_table()
    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT params, atualizado_em "
                "FROM public.tenant_routing_operacional_defaults "
                "WHERE tenant_id = %s",
                (tenant_id,),
            )
            row = cur.fetchone()
    if not row:
        return {"params": None, "atualizado_em": None}
    return {"params": row[0], "atualizado_em": _iso_utc(row[1])}


@router.put("/operacional/defaults", dependencies=[Depends(verify_token)])
async def save_routing_defaults_operacional(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]
    email = user.get("email") or "?"

    body = await request.json()
    params = body.get("params")
    if not isinstance(params, dict):
        raise HTTPException(400, "Body deve conter 'params' (objeto).")

    _ensure_routing_operacional_defaults_table()
    import json

    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.tenant_routing_operacional_defaults
                    (tenant_id, params, atualizado_em, atualizado_por)
                VALUES (%s, %s::jsonb, NOW(), %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET params = EXCLUDED.params,
                    atualizado_em = NOW(),
                    atualizado_por = EXCLUDED.atualizado_por;
                """,
                (tenant_id, json.dumps(params), email),
            )
        conn.commit()
    logger.info(f"🎛️ Routing operacional defaults salvos | tenant={tenant_id}")
    return {"status": "saved", "tenant_id": tenant_id}


# ============================================================
# 📅 Criar agenda a partir de uma roteirização operacional
# ============================================================
class CriarAgendaOperacionalRequest(BaseModel):
    nome: str
    data_inicio: str  # YYYY-MM-DD
    data_fim: str


@router.post(
    "/operacional/{routing_id}/criar-agenda",
    dependencies=[Depends(verify_token)],
    status_code=201,
)
def criar_agenda_operacional(
    request: Request, routing_id: str, body: CriarAgendaOperacionalRequest
):
    """Cria uma agenda (public.agenda*) a partir das rotas de uma
    roteirização operacional. O consultor de cada rota vem do setor
    (operacional.cluster_setor.consultor_nome). A persistência da agenda é
    delegada ao routing_engine via /internal/agenda/from-rotas."""
    tenant_id = request.state.user["tenant_id"]
    nome = (body.nome or "").strip()
    if not nome:
        raise HTTPException(400, "Informe um nome para a agenda.")

    with get_connection_context(schema=_OP) as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT ss.cluster_id, ss.subcluster_seq, ss.n_pdvs,
                       ss.dist_total_km, ss.tempo_total_min,
                       cs.consultor_nome, cs.cluster_label
                FROM sales_subcluster ss
                JOIN cluster_setor cs ON cs.id = ss.cluster_id
                WHERE ss.tenant_id = %s AND ss.routing_id = %s
                ORDER BY ss.cluster_id, ss.subcluster_seq;
                """,
                (tenant_id, routing_id),
            )
            sub_rows = cur.fetchall()
            if not sub_rows:
                raise HTTPException(
                    404, "Roteirização operacional não encontrada."
                )

            cur.execute(
                """
                SELECT spp.cluster_id, spp.subcluster_seq,
                       spp.sequencia_ordem, spp.lat, spp.lon,
                       p.cnpj, p.nome_fantasia, p.cidade, p.uf,
                       p.logradouro, p.numero, p.bairro, p.cep,
                       p.razao_social
                FROM sales_subcluster_pdv spp
                LEFT JOIN pdvs p ON p.id = spp.pdv_id
                WHERE spp.tenant_id = %s AND spp.routing_id = %s
                ORDER BY spp.cluster_id, spp.subcluster_seq,
                         spp.sequencia_ordem;
                """,
                (tenant_id, routing_id),
            )
            pdv_rows = cur.fetchall()

    from collections import defaultdict

    visitas: dict = defaultdict(list)
    for r in pdv_rows:
        visitas[(r[0], r[1])].append(
            {
                "sequencia": int(r[2] or 0),
                "lat": _f(r[3]),
                "lon": _f(r[4]),
                "cnpj": r[5],
                "nome_fantasia": r[6],
                "cidade": r[7],
                "uf": r[8],
                "logradouro": r[9],
                "numero": r[10],
                "bairro": r[11],
                "cep": r[12],
                "razao_social": r[13],
            }
        )

    rotas = []
    for r in sub_rows:
        cluster_id, seq = r[0], int(r[1] or 0)
        consultor = r[5] or f"Setor {r[6]}"
        rotas.append(
            {
                "consultor": consultor,
                "rota_id": f"R{seq}",
                "distancia_km": _f(r[3]),
                "tempo_min": _f(r[4]),
                "qtd_pdvs": int(r[2] or 0),
                "pdvs": visitas.get((cluster_id, seq), []),
            }
        )

    from src.sales_routing.infrastructure.routing_engine_client import (
        RoutingEngineClient,
    )

    client = RoutingEngineClient()
    if not client.enabled:
        raise HTTPException(503, "routing_engine não configurado.")
    try:
        return client.criar_agenda_from_rotas(
            {
                "tenant_id": tenant_id,
                "job_id": routing_id,
                "nome": nome,
                "data_inicio": body.data_inicio,
                "data_fim": body.data_fim,
                "rotas": rotas,
            }
        )
    except Exception as e:
        import requests as _rq

        if isinstance(e, _rq.HTTPError) and e.response is not None:
            try:
                detalhe = e.response.json().get("detail", str(e))
            except Exception:
                detalhe = e.response.text or str(e)
            raise HTTPException(e.response.status_code, detalhe)
        raise HTTPException(502, f"Falha ao criar agenda: {e}")


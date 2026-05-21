# sales_router/src/sales_clusterization/api/operacional_routes.py
#
# Router da SETORIZAÇÃO da Execução Operacional — endpoints sob
# /cluster/operacional/*. Persiste no schema `operacional`; usa
# consultores cadastrados como centros (algoritmo consultor_nearest).
#
# A setorização operacional roda SÍNCRONA (consultor_nearest é leve) —
# não há job RQ. O endpoint /clusterizar devolve o resultado direto.

import io
import json
import math
from datetime import timezone

from fastapi import APIRouter, Depends, HTTPException, Query, Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import StreamingResponse
from loguru import logger

from database.db_connection import get_connection
from src.sales_clusterization.application.setorizacao_operacional import (
    executar_setorizacao_operacional,
)
from src.sales_clusterization.reporting.export_operacional_pdv_detalhado_xlsx import (
    operacional_pdv_detalhado_to_bytes,
)

from .dependencies import verify_token

router = APIRouter()


def _conn_operacional():
    """Conexão com search_path = operacional, public."""
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute("SET search_path TO operacional, public")
    return conn


def _f(v):
    """float seguro para JSON (NaN/Inf → None)."""
    if v is None:
        return None
    try:
        x = float(v)
    except (TypeError, ValueError):
        return None
    return x if math.isfinite(x) else None


def _xlsx_response(payload: bytes, filename: str) -> StreamingResponse:
    """Devolve um XLSX (gerado em memória) como download."""
    return StreamingResponse(
        io.BytesIO(payload),
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


def _iso_utc(dt):
    """ISO 8601 com offset UTC explícito.

    cluster_run.criado_em / finished_at são `timestamp` naive guardando
    horário UTC (NOW() com a sessão em UTC). Sem o offset, o front
    interpreta a string como horário local e um run das 21h BRT aparece
    como 00h do dia seguinte — somindo dos filtros de data."""
    if dt is None:
        return None
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.isoformat()


# ============================================================
# 🩺 Health
# ============================================================
@router.get("/operacional/health", tags=["Operacional"])
def health():
    return {"status": "ok", "schema": "operacional"}


# ============================================================
# 📦 Carregamentos disponíveis para setorização operacional
# ============================================================
@router.get(
    "/operacional/inputs",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def listar_inputs_operacional(
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
    # historico_pdv_jobs.criado_em é timestamptz (UTC); o filtro DE/ATÉ é
    # escolhido no fuso local de operação (America/Sao_Paulo).
    filtro = """
          AND (%s IS NULL OR h.criado_em
               >= (%s::timestamp AT TIME ZONE 'America/Sao_Paulo'))
          AND (%s IS NULL OR h.criado_em
               <  ((%s::date + 1)::timestamp AT TIME ZONE 'America/Sao_Paulo'))
          AND (%s IS NULL OR h.descricao ILIKE %s)
    """
    args_filtro = (data_inicio, data_inicio, data_fim, data_fim, busca, busca)

    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM (
                    SELECT h.input_id
                    FROM historico_pdv_jobs h
                    WHERE h.tenant_id = %s AND h.status = 'done'
                    {filtro}
                    GROUP BY h.input_id
                ) t;
                """,
                (tenant_id, *args_filtro),
            )
            total = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                SELECT h.input_id,
                       MAX(h.criado_em) AS criado_em,
                       MIN(h.descricao) AS descricao,
                       MIN(p.uf)        AS uf,
                       MIN(p.cidade)    AS cidade,
                       COUNT(p.id)      AS total_pdvs
                FROM historico_pdv_jobs h
                LEFT JOIN pdvs p
                  ON p.tenant_id = h.tenant_id AND p.input_id = h.input_id
                WHERE h.tenant_id = %s AND h.status = 'done'
                {filtro}
                GROUP BY h.input_id
                ORDER BY criado_em DESC
                LIMIT %s OFFSET %s;
                """,
                (tenant_id, *args_filtro, limit, offset),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    inputs = [
        {
            "input_id": str(r[0]),
            "criado_em": r[1].isoformat() if r[1] else None,
            "descricao": r[2],
            "uf": r[3],
            "cidade": r[4],
            "total_pdvs": int(r[5] or 0),
        }
        for r in rows
    ]
    return {"total": total, "inputs": inputs}


# ============================================================
# 🎛️ Parâmetros padrão da setorização operacional (por tenant)
# ============================================================
# Um conjunto único de parâmetros padrão por tenant — espelha o que a
# Simulação faz em tenant_cluster_defaults. A UI carrega ao abrir a aba
# e o usuário pode salvar a qualquer momento. Tabela via lazy migration.
def _ensure_operacional_defaults_table() -> None:
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS
                public.tenant_setorizacao_operacional_defaults (
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


@router.get(
    "/operacional/parametros-padrao",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def get_parametros_padrao_operacional(request: Request):
    """Parâmetros padrão do tenant (ou null se nunca salvou)."""
    tenant_id = request.state.user["tenant_id"]
    _ensure_operacional_defaults_table()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT params, atualizado_em
                FROM public.tenant_setorizacao_operacional_defaults
                WHERE tenant_id = %s
                """,
                (tenant_id,),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row:
        return {"params": None, "atualizado_em": None}
    return {
        "params": row[0],
        "atualizado_em": _iso_utc(row[1]),
    }


@router.put(
    "/operacional/parametros-padrao",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
async def save_parametros_padrao_operacional(request: Request):
    """Salva (UPSERT) os parâmetros padrão do tenant."""
    user = request.state.user
    tenant_id = user["tenant_id"]
    email = user.get("email") or "?"

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido no body.")
    params = body.get("params")
    if not isinstance(params, dict):
        raise HTTPException(400, "Body deve conter 'params' (objeto).")

    _ensure_operacional_defaults_table()
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO public.tenant_setorizacao_operacional_defaults
                    (tenant_id, params, atualizado_em, atualizado_por)
                VALUES (%s, %s::jsonb, NOW(), %s)
                ON CONFLICT (tenant_id) DO UPDATE
                SET params = EXCLUDED.params,
                    atualizado_em = NOW(),
                    atualizado_por = EXCLUDED.atualizado_por;
                """,
                (tenant_id, json.dumps(params, ensure_ascii=False), email),
            )
        conn.commit()
        logger.info(
            f"🎛️ Parâmetros padrão operacionais salvos | tenant={tenant_id} "
            f"| por={email}"
        )
        return {"status": "saved", "tenant_id": tenant_id}
    finally:
        conn.close()


# ============================================================
# 🚀 Iniciar setorização operacional (SÍNCRONA)
# ============================================================
@router.post(
    "/operacional/clusterizar",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
async def clusterizar_operacional(request: Request):
    tenant_id = request.state.user["tenant_id"]
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(400, "JSON inválido no body.")

    input_id = str(body.get("input_id") or "").strip()
    if not input_id:
        raise HTTPException(400, "Informe o carregamento (input_id).")

    descricao = str(body.get("descricao") or "").strip()[:120]
    if not descricao:
        raise HTTPException(400, "Informe o apelido da setorização.")

    uf = body.get("uf")
    if not uf:
        raise HTTPException(400, "Informe ao menos uma UF.")

    consultor_ids = body.get("consultor_ids") or []
    if not consultor_ids:
        raise HTTPException(400, "Selecione ao menos um consultor.")

    cidade = body.get("cidade") or None
    # UF lista com >1 item → cidade não se aplica.
    if isinstance(uf, list) and len(uf) > 1:
        cidade = None

    # max/min só contam se preenchidos pelo usuário.
    max_pdv = body.get("max_pdv_cluster")
    min_pdv = body.get("min_pdv_cluster")
    max_pdv = int(max_pdv) if max_pdv not in (None, "") else None
    min_pdv = int(min_pdv) if min_pdv not in (None, "") else None

    # Demais parâmetros (dias_uteis, workday_min, etc.) seguem no snapshot.
    params = {
        k: body.get(k)
        for k in (
            "dias_uteis", "freq", "workday_min", "route_km_max",
            "service_min", "v_kmh", "permitir_excedente",
        )
        if body.get(k) is not None
    }

    try:
        resultado = await run_in_threadpool(
            executar_setorizacao_operacional,
            tenant_id=tenant_id,
            input_id=input_id,
            uf=uf,
            cidade=cidade,
            descricao=descricao,
            consultor_ids=consultor_ids,
            max_pdv=max_pdv,
            min_pdv=min_pdv,
            params=params,
        )
    except ValueError as e:
        raise HTTPException(400, str(e))
    except Exception as e:
        logger.error(f"[OPERACIONAL][CLUSTERIZAR][ERRO] {e}", exc_info=True)
        raise HTTPException(500, f"Falha na setorização: {e}")

    return resultado


# ============================================================
# 🗂️ Histórico de setorizações operacionais
# ============================================================
@router.get(
    "/operacional/historico",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def listar_historico_operacional(
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
    # cluster_run.criado_em é timestamp naive guardando UTC — interpreta
    # como UTC e converte ao fuso local antes de filtrar pela data.
    filtro = """
          AND (%s IS NULL OR
               (cr.criado_em AT TIME ZONE 'UTC'
                  AT TIME ZONE 'America/Sao_Paulo')::date >= %s::date)
          AND (%s IS NULL OR
               (cr.criado_em AT TIME ZONE 'UTC'
                  AT TIME ZONE 'America/Sao_Paulo')::date <= %s::date)
          AND (%s IS NULL OR cr.descricao ILIKE %s)
    """
    args_filtro = (data_inicio, data_inicio, data_fim, data_fim, busca, busca)

    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT COUNT(*) FROM cluster_run cr
                WHERE cr.tenant_id = %s
                {filtro};
                """,
                (tenant_id, *args_filtro),
            )
            total = int(cur.fetchone()[0] or 0)

            cur.execute(
                f"""
                SELECT
                    cr.criado_em,
                    cr.clusterization_id,
                    cr.descricao,
                    cr.status,
                    cr.uf,
                    cr.cidade,
                    cr.algo,
                    cr.k_final,
                    cr.desatualizado,
                    EXTRACT(EPOCH FROM (cr.finished_at - cr.criado_em))::int
                        AS duracao_segundos,
                    COUNT(cs.id)::int                AS qtd_clusters,
                    COALESCE(SUM(cs.n_pdvs), 0)::int  AS pdvs_total
                FROM cluster_run cr
                LEFT JOIN cluster_setor cs ON cs.run_id = cr.id
                WHERE cr.tenant_id = %s
                {filtro}
                GROUP BY cr.id
                ORDER BY cr.criado_em DESC
                LIMIT %s OFFSET %s;
                """,
                (tenant_id, *args_filtro, limit, offset),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    cz = [
        {
            "criado_em": _iso_utc(r[0]),
            "clusterization_id": str(r[1]),
            "descricao": r[2],
            "status": r[3],
            "uf": r[4],
            "cidade": r[5],
            "algo": r[6],
            "k_final": r[7],
            "desatualizado": bool(r[8]),
            "duracao_segundos": r[9],
            "qtd_clusters": int(r[10] or 0),
            "pdvs_total": int(r[11] or 0),
        }
        for r in rows
    ]
    return {"total": total, "clusterizacoes": cz}


# ============================================================
# 🗺️ Pontos + centros de uma setorização operacional
# ============================================================
@router.get(
    "/operacional/{clusterization_id}/pontos",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def listar_pontos_operacional(
    request: Request,
    clusterization_id: str,
    limit: int = 5000,
):
    tenant_id = request.state.user["tenant_id"]
    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id FROM cluster_run
                WHERE tenant_id = %s AND clusterization_id = %s
                ORDER BY criado_em DESC LIMIT 1;
                """,
                (tenant_id, clusterization_id),
            )
            row = cur.fetchone()
            if not row:
                raise HTTPException(404, "Setorização não encontrada.")
            run_id = int(row[0])

            cur.execute(
                """
                SELECT csp.lat, csp.lon, cs.cluster_label,
                       p.cnpj, p.cidade, p.uf,
                       p.pdv_endereco_completo, p.logradouro, p.numero,
                       p.bairro, p.cep, p.pdv_vendas, p.status_geolocalizacao,
                       p.razao_social, p.nome_fantasia
                FROM cluster_setor_pdv csp
                JOIN cluster_setor cs ON cs.id = csp.cluster_id
                LEFT JOIN pdvs p ON p.id = csp.pdv_id
                WHERE csp.tenant_id = %s AND csp.run_id = %s
                  AND csp.lat IS NOT NULL AND csp.lon IS NOT NULL
                LIMIT %s;
                """,
                (tenant_id, run_id, int(limit)),
            )
            pontos = []
            for r in cur.fetchall():
                lat, lon = _f(r[0]), _f(r[1])
                if lat is None or lon is None:
                    continue
                pontos.append({
                    "lat": lat, "lon": lon, "cluster_label": int(r[2]),
                    "cnpj": r[3], "cidade": r[4], "uf": r[5],
                    "endereco": r[6], "logradouro": r[7], "numero": r[8],
                    "bairro": r[9], "cep": r[10], "pdv_vendas": _f(r[11]),
                    "status_geolocalizacao": r[12],
                    "razao_social": r[13], "nome_fantasia": r[14],
                })

            cur.execute(
                """
                WITH agg AS (
                    SELECT csp.cluster_id,
                           COALESCE(SUM(p.pdv_vendas), 0)::float AS total_vendas
                    FROM cluster_setor_pdv csp
                    LEFT JOIN pdvs p ON p.id = csp.pdv_id
                    WHERE csp.tenant_id = %s AND csp.run_id = %s
                    GROUP BY csp.cluster_id
                )
                SELECT cs.cluster_label, cs.centro_lat, cs.centro_lon,
                       cs.n_pdvs, COALESCE(agg.total_vendas, 0)::float,
                       cs.consultor_nome, cs.metrics->>'banda_status'
                FROM cluster_setor cs
                LEFT JOIN agg ON agg.cluster_id = cs.id
                WHERE cs.run_id = %s
                ORDER BY cs.cluster_label;
                """,
                (tenant_id, run_id, run_id),
            )
            centros = [
                {
                    "cluster_label": int(r[0]),
                    "lat": _f(r[1]), "lon": _f(r[2]),
                    "n_pdvs": int(r[3] or 0),
                    "total_vendas": _f(r[4]) or 0.0,
                    "consultor_nome": r[5],
                    "banda_status": r[6],
                }
                for r in cur.fetchall()
            ]
    finally:
        conn.close()

    return {
        "clusterization_id": clusterization_id,
        "run_id": run_id,
        "total_pontos": len(pontos),
        "pontos": pontos,
        "centros": centros,
    }


# ============================================================
# 🎛️ Parâmetros usados em uma setorização operacional
# ============================================================
@router.get(
    "/operacional/{clusterization_id}/params",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def get_params_operacional(request: Request, clusterization_id: str):
    tenant_id = request.state.user["tenant_id"]
    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, algo, uf, cidade, criado_em, finished_at,
                       k_final, status, params, descricao, desatualizado
                FROM cluster_run
                WHERE tenant_id = %s AND clusterization_id = %s
                ORDER BY criado_em DESC LIMIT 1;
                """,
                (tenant_id, clusterization_id),
            )
            row = cur.fetchone()
    finally:
        conn.close()

    if not row:
        raise HTTPException(404, "Setorização não encontrada.")

    params_raw = row[8]
    if isinstance(params_raw, str):
        import json
        try:
            params_raw = json.loads(params_raw)
        except Exception:
            params_raw = {"_raw": params_raw}

    return {
        "clusterization_id": clusterization_id,
        "run_id": int(row[0]),
        "algo": row[1],
        "uf": row[2],
        "cidade": row[3],
        "criado_em": _iso_utc(row[4]),
        "finished_at": _iso_utc(row[5]),
        "k_final": row[6],
        "status": row[7],
        "descricao": row[9],
        "desatualizado": bool(row[10]),
        "params": params_raw,
    }


# ============================================================
# 📥 Export XLSX detalhado — uma linha por PDV (gerado on-demand)
# ============================================================
@router.get(
    "/operacional/{clusterization_id}/export-detalhado",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def export_detalhado_operacional(request: Request, clusterization_id: str):
    """XLSX com 1 linha por PDV — setor, consultor e todos os detalhes.
    Gerado em memória a cada chamada; nada é persistido em disco."""
    tenant_id = request.state.user["tenant_id"]
    try:
        data = operacional_pdv_detalhado_to_bytes(tenant_id, clusterization_id)
    except ValueError as e:
        raise HTTPException(404, str(e))
    except Exception as e:
        logger.error(f"[OPERACIONAL][EXPORT][ERRO] {e}", exc_info=True)
        raise HTTPException(500, f"Falha ao gerar o XLSX: {e}")
    return _xlsx_response(
        data, f"setorizacao_operacional_{clusterization_id}.xlsx"
    )

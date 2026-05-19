#sales_router/src/sales_clusterization/api/routes.py

# ============================================================
# 📦 src/sales_clusterization/api/routes.py
# ============================================================

from fastapi import APIRouter, Query, Depends, Request, HTTPException
from fastapi.responses import StreamingResponse
from loguru import logger
from redis import Redis
from rq import Queue
from rq.job import Job
from sales_clusterization.reporting.export_cluster_pdv_detalhado_xlsx import (
    cluster_pdv_detalhado_to_bytes,
)
from .dependencies import verify_token
import io
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
    # OBS: `criado_em` é TIMESTAMPTZ (armazena UTC). Filtramos pelo
    # horário local America/Sao_Paulo pra evitar perder uploads feitos
    # após 21h BRT (que em UTC já viram dia seguinte).
    # ============================================================
    sql_total = """
        SELECT COUNT(DISTINCT h.input_id)
        FROM historico_pdv_jobs h
        WHERE h.tenant_id = %s
          AND h.status = 'done'
          AND (%s IS NULL OR h.criado_em >= (%s::timestamp AT TIME ZONE 'America/Sao_Paulo'))
          AND (%s IS NULL OR h.criado_em <  ((%s::date + 1)::timestamp AT TIME ZONE 'America/Sao_Paulo'))
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
          AND (%s IS NULL OR h.criado_em >= (%s::timestamp AT TIME ZONE 'America/Sao_Paulo'))
          AND (%s IS NULL OR h.criado_em <  ((%s::date + 1)::timestamp AT TIME ZONE 'America/Sao_Paulo'))
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

    algo = body.get("algo", "kmeans")
    # Cidade é opcional em todos os algoritmos. Quando omitida, o algoritmo
    # roda sobre todos os PDVs da UF (pode incluir várias cidades).
    # Pré-gera clusterization_id no endpoint pra retornar ao frontend (que usa
    # pra auto-selecionar a execução assim que ela aparece no histórico).
    import uuid as _uuid
    clusterization_id = str(_uuid.uuid4())

    # UF aceita string ("MG") ou lista (["MG","SP"]). Quando lista com >1
    # item, cidade é forçada a None — não faz sentido restringir uma cidade
    # quando a setorização cobre múltiplas UFs.
    uf_in = body.get("uf")
    cidade_in = body.get("cidade")
    if isinstance(uf_in, list) and len(uf_in) > 1:
        cidade_in = None

    params = {
        "tenant_id": tenant_id,
        "uf": uf_in,
        "cidade": cidade_in,
        "algo": body.get("algo", "kmeans"),
        "descricao": body["descricao"],
        "input_id": body["input_id"],
        "clusterization_id": clusterization_id,

        "max_pdv_cluster": body.get("max_pdv_cluster", 200),
        # "operacional" (default, atual): kmeans_balanceado + refinador de rotas diárias
        # "capacidade": só kmeans_balanceado (respeita teto de PDVs, ignora workday/route)
        "modo_refinamento": body.get("modo_refinamento", "operacional"),
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

    return {
        "status": "queued",
        "job_id": job.id,
        "clusterization_id": clusterization_id,
    }

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
        filtros.append("DATE(h.criado_em AT TIME ZONE 'America/Sao_Paulo') >= %s")
        where_params.append(data_inicio)

    if data_fim:
        filtros.append("DATE(h.criado_em AT TIME ZONE 'America/Sao_Paulo') <= %s")
        where_params.append(data_fim)

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
        filtros.append("DATE(h.criado_em AT TIME ZONE 'America/Sao_Paulo') >= %s")
        where_params.append(data_inicio)

    if data_fim:
        filtros.append("DATE(h.criado_em AT TIME ZONE 'America/Sao_Paulo') <= %s")
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
# XLSX é gerado on-demand em memória (BytesIO) e devolvido via
# StreamingResponse. Não persiste mais em /app/output/reports/.
def _xlsx_response(payload: bytes, filename: str) -> StreamingResponse:
    return StreamingResponse(
        io.BytesIO(payload),
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# Endpoint /export/resumo removido em 2026-05-18: a aba "Setores" na UI
# já mostra o mesmo resumo (n_pdvs, centro, %) sem precisar baixar XLSX.
# Pra resumos via XLSX, use o detalhado abaixo (que inclui tudo + isolados).


# ============================================================
# 📊 Exportar detalhado XLSX
# ============================================================
@router.get("/export/detalhado", dependencies=[Depends(verify_token)], tags=["Relatórios"])
def export_detalhado(request: Request, clusterization_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]
    try:
        data = cluster_pdv_detalhado_to_bytes(tenant_id, clusterization_id)
        return _xlsx_response(
            data, f"cluster_pdv_detalhado_{clusterization_id}.xlsx"
        )
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Endpoint POST /mapa removido em 2026-05-18: gerava HTML Folium estático
# em /app/output/maps/ (1 por execução, somava ~1.8GB). A UI nova usa o
# endpoint GET /cluster/{id}/pontos abaixo + Leaflet inline no frontend.


# ============================================================
# 📍 Pontos da setorização (para mapa Leaflet inline no frontend)
# ============================================================
# Substitui a necessidade do HTML estático Folium. Frontend consome
# diretamente e renderiza com cores por cluster_label.
@router.get(
    "/{clusterization_id}/pontos",
    dependencies=[Depends(verify_token)],
    tags=["Visualização"],
)
def listar_pontos(
    request: Request,
    clusterization_id: str,
    limit: int = Query(default=5000, le=20000),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id AS run_id
            FROM cluster_run
            WHERE tenant_id = %s AND clusterization_id = %s
            ORDER BY criado_em DESC
            LIMIT 1;
            """,
            (tenant_id, clusterization_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Setorização não encontrada para este tenant.")
        run_id = int(row[0])

        import math

        def _f(v):
            """Converte pra float seguro pra JSON (NaN/Inf → None).
            pdv_vendas e até lat/lon podem vir como NaN se a base for
            inconsistente — sem essa proteção o FastAPI levanta
            ValueError no JSON encode (Out of range float values)."""
            if v is None:
                return None
            try:
                x = float(v)
            except (TypeError, ValueError):
                return None
            return x if math.isfinite(x) else None

        # Pontos individuais (cluster_setor_pdv + pdvs) — payload enxuto
        # mas suficiente pra popup completo no mapa (endereço, vendas, status).
        cur.execute(
            """
            SELECT csp.lat, csp.lon, cs.cluster_label,
                   p.cnpj, p.cidade, p.uf,
                   p.pdv_endereco_completo, p.logradouro, p.numero,
                   p.bairro, p.cep, p.pdv_vendas, p.status_geolocalizacao
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
            lat = _f(r[0])
            lon = _f(r[1])
            if lat is None or lon is None:
                continue
            pontos.append(
                {
                    "lat": lat,
                    "lon": lon,
                    "cluster_label": int(r[2]),
                    "cnpj": r[3],
                    "cidade": r[4],
                    "uf": r[5],
                    "endereco": r[6],
                    "logradouro": r[7],
                    "numero": r[8],
                    "bairro": r[9],
                    "cep": r[10],
                    "pdv_vendas": _f(r[11]),
                    "status_geolocalizacao": r[12],
                }
            )

        # Centros dos setores — agora enriquecidos com:
        #  - cidade_dominante / uf_dominante: a cidade que aparece mais nos
        #    PDVs do setor (usado na tabela "Setores" pra rotular o setor
        #    sem precisar de reverse-geocoding do centroide).
        #  - total_vendas: soma de pdv_vendas dos PDVs do setor (receita).
        cur.execute(
            """
            WITH agg AS (
                SELECT
                    csp.cluster_id,
                    COALESCE(SUM(p.pdv_vendas), 0)::float AS total_vendas
                FROM cluster_setor_pdv csp
                LEFT JOIN pdvs p ON p.id = csp.pdv_id
                WHERE csp.tenant_id = %s AND csp.run_id = %s
                GROUP BY csp.cluster_id
            ),
            cidade_ranqueada AS (
                SELECT
                    csp.cluster_id,
                    p.cidade,
                    p.uf,
                    COUNT(*) AS cnt,
                    ROW_NUMBER() OVER (
                        PARTITION BY csp.cluster_id
                        ORDER BY COUNT(*) DESC
                    ) AS rn
                FROM cluster_setor_pdv csp
                JOIN pdvs p ON p.id = csp.pdv_id
                WHERE csp.tenant_id = %s AND csp.run_id = %s
                  AND p.cidade IS NOT NULL AND p.uf IS NOT NULL
                GROUP BY csp.cluster_id, p.cidade, p.uf
            )
            SELECT
                cs.cluster_label,
                cs.centro_lat,
                cs.centro_lon,
                cs.n_pdvs,
                COALESCE(agg.total_vendas, 0)::float AS total_vendas,
                cd.cidade AS cidade_dominante,
                cd.uf AS uf_dominante
            FROM cluster_setor cs
            LEFT JOIN agg ON agg.cluster_id = cs.id
            LEFT JOIN cidade_ranqueada cd
                ON cd.cluster_id = cs.id AND cd.rn = 1
            WHERE cs.run_id = %s
            ORDER BY cs.cluster_label;
            """,
            (tenant_id, run_id, tenant_id, run_id, run_id),
        )
        centros = [
            {
                "cluster_label": int(r[0]),
                "lat": _f(r[1]),
                "lon": _f(r[2]),
                "n_pdvs": int(r[3] or 0),
                "total_vendas": _f(r[4]) or 0.0,
                "cidade_dominante": r[5],
                "uf_dominante": r[6],
            }
            for r in cur.fetchall()
        ]

        return {
            "clusterization_id": clusterization_id,
            "run_id": run_id,
            "total_pontos": len(pontos),
            "pontos": pontos,
            "centros": centros,
        }
    finally:
        conn.close()


# ============================================================
# 🎛️ Defaults de parâmetros do tenant (Parâmetros salvos)
# ============================================================
# Permite que o tenant salve um conjunto padrão de parâmetros da
# setorização (algo, modo_refinamento, max_pdv_cluster, dias_uteis etc).
# A UI carrega esses defaults ao abrir o modal e o usuário pode salvar/
# atualizar a qualquer momento. Tabela criada via lazy migration —
# segue padrão do projeto (pdv_invalidos, viacep_cache).
def _ensure_tenant_defaults_table():
    from database.db_connection import get_connection

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS tenant_cluster_defaults (
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
    "/defaults",
    dependencies=[Depends(verify_token)],
    tags=["Configuração"],
)
def get_cluster_defaults(request: Request):
    """Retorna os defaults de parâmetros do tenant (ou {} se nunca salvou)."""
    user = request.state.user
    tenant_id = user["tenant_id"]

    _ensure_tenant_defaults_table()

    from database.db_connection import get_connection

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT params, atualizado_em FROM tenant_cluster_defaults WHERE tenant_id = %s",
            (tenant_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"params": None, "atualizado_em": None}
        return {"params": row[0], "atualizado_em": row[1].isoformat() if row[1] else None}
    finally:
        conn.close()


@router.put(
    "/defaults",
    dependencies=[Depends(verify_token)],
    tags=["Configuração"],
)
async def save_cluster_defaults(request: Request):
    """Salva (UPSERT) os defaults de parâmetros do tenant."""
    user = request.state.user
    tenant_id = user["tenant_id"]
    email = user.get("email") or "?"

    body = await request.json()
    params = body.get("params")
    if not isinstance(params, dict):
        raise HTTPException(400, "Body deve conter 'params' (objeto).")

    _ensure_tenant_defaults_table()

    from database.db_connection import get_connection
    import json

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO tenant_cluster_defaults (tenant_id, params, atualizado_em, atualizado_por)
            VALUES (%s, %s::jsonb, NOW(), %s)
            ON CONFLICT (tenant_id) DO UPDATE
            SET params = EXCLUDED.params,
                atualizado_em = NOW(),
                atualizado_por = EXCLUDED.atualizado_por;
            """,
            (tenant_id, json.dumps(params), email),
        )
        conn.commit()
        logger.info(f"🎛️ Defaults salvos | tenant={tenant_id} | por={email}")
        return {"status": "saved", "tenant_id": tenant_id}
    finally:
        conn.close()


# ============================================================
# 📜 Snapshot dos parâmetros usados em uma setorização
# ============================================================
# Lê o JSON `params` do cluster_run mais recente do clusterization_id.
# O snapshot é gravado por snapshot_params() durante a execução, então
# reflete exatamente o que foi rodado (independente dos defaults atuais).
@router.get(
    "/{clusterization_id}/params",
    dependencies=[Depends(verify_token)],
    tags=["Visualização"],
)
def get_params_clusterizacao(request: Request, clusterization_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]

    from database.db_connection import get_connection

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT id AS run_id, algo, uf, cidade, criado_em, finished_at,
                   k_final, status, params, descricao
            FROM cluster_run
            WHERE tenant_id = %s AND clusterization_id = %s
            ORDER BY criado_em DESC
            LIMIT 1;
            """,
            (tenant_id, clusterization_id),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(404, "Setorização não encontrada para este tenant.")

        params_raw = row[8]
        # `params` pode vir como dict (jsonb) ou string JSON — normaliza.
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
            "criado_em": row[4].isoformat() if row[4] else None,
            "finished_at": row[5].isoformat() if row[5] else None,
            "k_final": row[6],
            "status": row[7],
            "descricao": row[9],
            "params": params_raw,
        }
    finally:
        conn.close()

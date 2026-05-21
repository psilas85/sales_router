# sales_router/src/pdv_preprocessing/api/operacional_routes.py
#
# Router da pipeline de Geocodificação da EXECUÇÃO OPERACIONAL.
#
# Espelha os endpoints de carregamento da Simulação Inteligente
# (pdv_preprocessing/api/routes.py), mas persiste no schema `operacional`
# em vez de `public`. É aditivo: o router de Simulação fica intocado.
#
# Mecanismo: as conexões deste router rodam com search_path
# `operacional, public` — assim `pdvs`/`historico_pdv_jobs`/`pdv_invalidos`
# resolvem no schema operacional, enquanto `cadastro_pdvs`/`enderecos_cache`
# (só existem em public) seguem resolvendo normalmente.
#
# Para o upload XLSX, o job RQ `processar_pdv` é enfileirado com
# schema="operacional" — o worker repassa `--schema operacional` ao
# subprocesso, que instancia reader/writer no schema correto.

import io
import os
import re
import shutil
from datetime import datetime, timedelta
from uuid import UUID, uuid4

import numpy as np
import pandas as pd
from fastapi import (
    APIRouter,
    Body,
    Depends,
    File,
    HTTPException,
    Query,
    Request,
    UploadFile,
)
from fastapi.responses import StreamingResponse
from loguru import logger
from psycopg2.extras import execute_values
from redis import Redis
from rq import Queue
from rq.job import Job

from database.db_connection import get_connection
from pdv_preprocessing.api.dependencies import verify_token
from pdv_preprocessing.api.routes import (
    _cache_keys_geocoding,
    EditarLocalPayload,
    PdvsFromCadastroSchema,
    enrich_job_history_rows,
    parse_data,
    normalize_text,
    _normalizar_input_id,
    _normalizar_nome_coluna,
    _SORTABLE_ULTIMOS_JOBS,
)
from pdv_preprocessing.utils.file_utils import enrich_invalidos_for_export
from pdv_preprocessing.domain.utils_geo import coordenada_generica
from pdv_preprocessing.entities.pdv_entity import PDV
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.infrastructure.operacional_schema import (
    ensure_operacional_schema,
)
from pdv_preprocessing.pdv_jobs import processar_pdv

router = APIRouter()

SCHEMA = "operacional"


# ============================================================
# 🔌 Conexão com search_path no schema operacional
# ============================================================
def _conn_operacional():
    """Conexão (fresh) com search_path = operacional, public.

    cadastro_pdvs / enderecos_cache / viacep_cache continuam resolvendo
    em public (não existem em operacional). A conexão é descartada com
    conn.close() no fim de cada endpoint — sem risco de contaminar pool.
    """
    conn = get_connection()
    ensure_operacional_schema(conn)  # idempotente (guard interno)
    with conn.cursor() as cur:
        cur.execute("SET search_path TO operacional, public")
    conn.commit()
    return conn


def _input_id_valido(input_id: str) -> str:
    """Normaliza/valida um input_id (UUID). Levanta 400 se inválido."""
    try:
        return str(UUID(re.sub(r"[^a-fA-F0-9\-]", "", input_id)))
    except Exception:
        raise HTTPException(status_code=400, detail="input_id inválido")


# ============================================================
# 🩺 Health
# ============================================================
@router.get("/operacional/health", tags=["Operacional"])
def health():
    return {"status": "ok", "schema": SCHEMA}


# ============================================================
# 📤 Upload XLSX → job de geocodificação (assíncrono)
# ============================================================
@router.post(
    "/operacional/upload-file",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def upload_arquivo_operacional(
    request: Request,
    descricao: str = Query(..., description="Descrição amigável do job"),
    file: UploadFile = File(...),
):
    """Recebe XLSX/CSV, salva no volume e enfileira a geocodificação
    persistindo no schema operacional."""
    tenant_id = request.state.user["tenant_id"]

    try:
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in [".xlsx", ".xls", ".csv"]:
            raise HTTPException(
                status_code=400,
                detail="Formato inválido. Envie um arquivo XLSX ou CSV.",
            )

        base_dir = "/app/data"
        os.makedirs(base_dir, exist_ok=True)

        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        nome_final = f"operacional_{tenant_id}_{timestamp}_{file.filename}"
        caminho_final = os.path.join(base_dir, nome_final)

        with open(caminho_final, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        conn_redis = Redis(host="redis", port=6379)
        queue = Queue("pdv_jobs", connection=conn_redis)

        job = queue.enqueue(
            processar_pdv,
            tenant_id,
            caminho_final,
            descricao,
            SCHEMA,  # 4º arg posicional → schema do worker
            job_timeout=36000,
        )

        logger.info(
            f"[OPERACIONAL] upload enfileirado job={job.id} "
            f"tenant={tenant_id} arquivo={caminho_final}"
        )

        return {
            "status": "queued",
            "job_id": job.id,
            "tenant_id": tenant_id,
            "arquivo_salvo": caminho_final,
            "descricao": descricao,
            "arquivo_original": file.filename,
            "extensao": ext,
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[OPERACIONAL] erro no upload: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ============================================================
# 📇 Carregamento a partir do Cadastro de Clientes (síncrono)
# ============================================================
@router.post(
    "/operacional/carregamento-from-cadastro",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
async def carregamento_from_cadastro_operacional(request: Request):
    """Gera um carregamento operacional (input_id) a partir do Cadastro de
    Clientes — snapshot dos PDVs ATIVOS e GEOCODIFICADOS selecionados,
    direto para operacional.pdvs. Sem upload, sem geocodificação."""
    tenant_id = request.state.user["tenant_id"]

    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="JSON inválido no body.")

    descricao = str(body.get("descricao") or "").strip()[:60]
    if not descricao:
        raise HTTPException(
            status_code=400, detail="Informe a descrição do carregamento."
        )

    ids = [str(i).strip() for i in (body.get("cliente_ids") or []) if str(i).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="Nenhum cliente selecionado.")

    input_id = str(uuid4())
    job_id = str(uuid4())

    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            # cadastro_pdvs resolve em public (não existe em operacional).
            cur.execute(
                """
                SELECT cnpj, logradouro, numero, bairro, cidade, uf, cep,
                       pdv_lat, pdv_lon, status_geolocalizacao, pdv_vendas,
                       janela_atendimento_inicio, janela_atendimento_fim,
                       tempo_atendimento_min, is_estrategico,
                       razao_social, nome_fantasia
                FROM cadastro_pdvs
                WHERE tenant_id = %s
                  AND id = ANY(%s::uuid[])
                  AND ativo = TRUE
                  AND pdv_lat IS NOT NULL
                  AND pdv_lon IS NOT NULL
                """,
                (tenant_id, ids),
            )
            rows = cur.fetchall()

            if not rows:
                raise HTTPException(
                    status_code=400,
                    detail="Nenhum cliente ativo e geocodificado entre os selecionados.",
                )

            valores = []
            vistos: set[str] = set()
            for r in rows:
                (cnpj, log, num, bairro, cid, uf_, cep, lat, lon, status,
                 vendas, jini, jfim, tempo, estrat, rs, nf) = r
                if cnpj in vistos:
                    continue
                vistos.add(cnpj)
                canonico, chave = _cache_keys_geocoding(log, num, cid, uf_)
                valores.append((
                    tenant_id, input_id, descricao, cnpj,
                    log, num, bairro, cid, uf_, cep,
                    canonico, chave, lat, lon, status, vendas,
                    jini, jfim, tempo, estrat, rs, nf,
                ))

            # pdvs resolve em operacional.pdvs (search_path).
            execute_values(
                cur,
                """
                INSERT INTO pdvs (
                    tenant_id, input_id, descricao, cnpj,
                    logradouro, numero, bairro, cidade, uf, cep,
                    pdv_endereco_completo, endereco_cache_key,
                    pdv_lat, pdv_lon, status_geolocalizacao, pdv_vendas,
                    janela_atendimento_inicio, janela_atendimento_fim,
                    tempo_atendimento_min, is_estrategico,
                    razao_social, nome_fantasia
                ) VALUES %s
                ON CONFLICT (tenant_id, input_id, cnpj) DO NOTHING
                """,
                valores,
            )
            total = len(valores)

            cur.execute(
                """
                INSERT INTO historico_pdv_jobs (
                    tenant_id, job_id, arquivo, status,
                    total_processados, validos, invalidos,
                    inseridos, sobrescritos, mensagem, descricao,
                    input_id, origem, criado_em
                ) VALUES (%s,%s,%s,'done',%s,%s,0,%s,0,%s,%s,%s,'cadastro', NOW())
                """,
                (
                    tenant_id, job_id, "Cadastro de Clientes",
                    total, total, total,
                    "origem:cadastro_clientes", descricao, input_id,
                ),
            )

        conn.commit()
        logger.info(
            f"[OPERACIONAL][CARREGAMENTO_CADASTRO] input_id={input_id} "
            f"total={total} tenant={tenant_id} descricao='{descricao}'"
        )
        return {"input_id": input_id, "job_id": job_id, "total": total}

    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"[OPERACIONAL][CARREGAMENTO_CADASTRO][ERRO] {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Erro ao gerar carregamento: {e}"
        )
    finally:
        conn.close()


# ============================================================
# 🔢 Enriquecimento de contagens ao vivo (operacional.pdvs)
# ============================================================
def _enrich_jobs_live_counts_operacional(jobs: list, tenant_id: int) -> list:
    """Versão operacional de enrich_jobs_with_live_counts — conta em
    operacional.pdvs (via search_path)."""
    if not jobs:
        return jobs
    input_ids = [str(j["input_id"]) for j in jobs if j.get("input_id")]
    if not input_ids:
        for job in jobs:
            job["manuais"] = 0
        return jobs

    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT input_id::text,
                       COUNT(*) AS validos,
                       COUNT(*) FILTER (
                           WHERE status_geolocalizacao = 'manual_insert'
                       ) AS manuais
                FROM pdvs
                WHERE tenant_id = %s AND input_id::text = ANY(%s)
                GROUP BY input_id;
                """,
                (tenant_id, input_ids),
            )
            counts = {
                row[0]: {"validos": int(row[1]), "manuais": int(row[2])}
                for row in cur.fetchall()
            }
    finally:
        conn.close()

    for job in jobs:
        contagem = counts.get(str(job.get("input_id")))
        invalidos = int(job.get("invalidos") or 0)
        if contagem:
            job["validos"] = contagem["validos"]
            job["manuais"] = contagem["manuais"]
            job["total_processados"] = contagem["validos"] + invalidos
        else:
            job["manuais"] = 0
    return jobs


# ============================================================
# 📋 Carregamentos operacionais — paginado + ordenado
# (rotas estáticas ANTES da dinâmica /jobs/{job_id})
# ============================================================
@router.get(
    "/operacional/jobs/ultimos",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def listar_ultimos_jobs_operacional(
    request: Request,
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("criado_em"),
    sort_dir: str = Query("desc"),
):
    tenant_id = request.state.user["tenant_id"]

    sort_col = _SORTABLE_ULTIMOS_JOBS.get(sort_by, "criado_em")
    sort_dir_sql = "ASC" if str(sort_dir).lower() == "asc" else "DESC"
    order_by_clause = f"{sort_col} {sort_dir_sql} NULLS LAST, criado_em DESC"

    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                f"""
                SELECT id, tenant_id, job_id, arquivo, status,
                       total_processados, validos, invalidos,
                       arquivo_invalidos, mensagem, criado_em,
                       inseridos, sobrescritos, descricao, input_id
                FROM (
                    SELECT DISTINCT ON (input_id) *
                    FROM historico_pdv_jobs
                    WHERE tenant_id = %s
                    ORDER BY input_id, criado_em DESC
                ) t
                ORDER BY {order_by_clause}
                LIMIT %s OFFSET %s
                """,
                (tenant_id, limit, offset),
            )
            rows = cur.fetchall()
            cur.execute(
                "SELECT COUNT(DISTINCT input_id) FROM historico_pdv_jobs "
                "WHERE tenant_id = %s",
                (tenant_id,),
            )
            total = cur.fetchone()[0]
    finally:
        conn.close()

    colunas = [
        "id", "tenant_id", "job_id", "arquivo", "status",
        "total_processados", "validos", "invalidos", "arquivo_invalidos",
        "mensagem", "criado_em", "inseridos", "sobrescritos",
        "descricao", "input_id",
    ]
    jobs = [dict(zip(colunas, row)) for row in rows]
    jobs = enrich_job_history_rows(jobs)
    jobs = _enrich_jobs_live_counts_operacional(jobs, tenant_id)
    return {"total": total, "jobs": jobs}


@router.get(
    "/operacional/jobs/filtrar",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def filtrar_jobs_operacional(
    request: Request,
    data_inicio: str = Query(None),
    data_fim: str = Query(None),
    descricao: str = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    tenant_id = request.state.user["tenant_id"]

    data_inicio_dt = parse_data(data_inicio)
    data_fim_dt = parse_data(data_fim)

    filtros = ["tenant_id = %s"]
    params: list = [tenant_id]
    # criado_em é timestamptz (UTC). O filtro DE/ATÉ é escolhido pelo usuário
    # no fuso local (America/Sao_Paulo) — converte criado_em p/ o fuso local
    # antes de comparar. Sem isso, uploads do fim da tarde (após 21h BRT =
    # 00h UTC do dia seguinte) caem no dia UTC seguinte e somem do filtro.
    if data_inicio_dt:
        filtros.append("(criado_em AT TIME ZONE 'America/Sao_Paulo') >= %s")
        params.append(datetime.combine(data_inicio_dt, datetime.min.time()))
    if data_fim_dt:
        filtros.append("(criado_em AT TIME ZONE 'America/Sao_Paulo') < %s")
        params.append(
            datetime.combine(data_fim_dt + timedelta(days=1), datetime.min.time())
        )
    if descricao:
        filtros.append("descricao ILIKE %s")
        params.append(f"%{descricao}%")
    where_clause = " AND ".join(filtros)

    FILTRAR_JOBS_CAP = 100
    count_sql = (
        f"SELECT COUNT(*) FROM (SELECT DISTINCT input_id "
        f"FROM historico_pdv_jobs WHERE {where_clause}) t;"
    )
    safe_offset = max(0, min(offset, max(FILTRAR_JOBS_CAP - limit, 0)))
    sql = f"""
        SELECT *
        FROM (
            SELECT DISTINCT ON (input_id)
                tenant_id, job_id, input_id, descricao, arquivo, origem,
                status, total_processados, validos, invalidos,
                mensagem, criado_em
            FROM historico_pdv_jobs
            WHERE {where_clause}
            ORDER BY input_id, criado_em DESC
        ) t
        ORDER BY criado_em DESC
        LIMIT %s OFFSET %s;
    """

    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(count_sql, tuple(params))
            total = int(cur.fetchone()[0] or 0)
        df = pd.read_sql_query(
            sql, conn, params=tuple(params) + (limit, safe_offset)
        )
    finally:
        conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})
    jobs = enrich_job_history_rows(df.to_dict(orient="records"))
    jobs = _enrich_jobs_live_counts_operacional(jobs, tenant_id)
    return {"total": total, "jobs": jobs, "cap": FILTRAR_JOBS_CAP}


# ============================================================
# 🔎 Detalhe de um job operacional (resumo do carregamento)
# ============================================================
@router.get(
    "/operacional/jobs/{job_id}",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def detalhar_job_operacional(request: Request, job_id: str):
    tenant_id = request.state.user["tenant_id"]

    # 1) Fonte da verdade: operacional.historico_pdv_jobs
    try:
        conn = _conn_operacional()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    SELECT job_id, status, descricao, arquivo, input_id,
                           total_processados, validos, invalidos, inseridos,
                           sobrescritos, arquivo_invalidos, mensagem, criado_em
                    FROM historico_pdv_jobs
                    WHERE tenant_id = %s AND job_id = %s
                    LIMIT 1;
                    """,
                    (tenant_id, job_id),
                )
                row = cur.fetchone()
        finally:
            conn.close()

        if row:
            colunas = [
                "job_id", "status", "descricao", "arquivo", "input_id",
                "total_processados", "validos", "invalidos", "inseridos",
                "sobrescritos", "arquivo_invalidos", "mensagem", "criado_em",
            ]
            return dict(zip(colunas, row))
    except Exception as e:
        logger.warning(f"[OPERACIONAL] erro ao buscar job no banco: {e}")

    # 2) Fallback: Redis (job ainda em execução)
    try:
        job = Job.fetch(job_id, connection=Redis(host="redis", port=6379))
        return {
            "job_id": job.id,
            "status": job.get_status(),
            "meta": job.meta,
            "tenant_id": job.args[0] if job.args else None,
            "arquivo": job.args[1] if len(job.args) > 1 else None,
            "descricao": job.args[2] if len(job.args) > 2 else None,
        }
    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")


# ============================================================
# 🗺️ Pontos de um carregamento operacional (mapa Leaflet)
# ============================================================
@router.get(
    "/operacional/mapa-locais",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def mapa_locais_operacional(
    request: Request,
    input_id: str = Query(..., description="Input ID obrigatório"),
    limit: int = Query(1000, ge=1, le=1000),
):
    tenant_id = request.state.user["tenant_id"]
    input_id = _input_id_valido(input_id)

    conn = _conn_operacional()
    try:
        total_df = pd.read_sql_query(
            """
            SELECT COUNT(*) AS total
            FROM pdvs
            WHERE tenant_id = %s AND input_id = %s
              AND pdv_lat IS NOT NULL AND pdv_lon IS NOT NULL;
            """,
            conn,
            params=(tenant_id, input_id),
        )
        df = pd.read_sql_query(
            """
            SELECT id, cnpj,
                   pdv_lat AS lat, pdv_lon AS lon, cidade,
                   COALESCE(
                       NULLIF(TRIM(pdv_endereco_completo), ''),
                       CONCAT_WS(', ', logradouro, numero, bairro, cidade, uf, cep)
                   ) AS endereco
            FROM pdvs
            WHERE tenant_id = %s AND input_id = %s
              AND pdv_lat IS NOT NULL AND pdv_lon IS NOT NULL
            ORDER BY RANDOM()
            LIMIT %s;
            """,
            conn,
            params=(tenant_id, input_id, limit),
        )
    finally:
        conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})
    return {
        "total": int(total_df.iloc[0]["total"]) if not total_df.empty else 0,
        "pontos": df.to_dict(orient="records"),
    }


# ============================================================
# ✏️ Editar coordenada de um PDV operacional
# ============================================================
@router.put(
    "/operacional/locais/{pdv_id}",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def editar_local_operacional(
    request: Request,
    pdv_id: int,
    payload: EditarLocalPayload = Body(...),
):
    """Único endpoint que altera lat/lon de um PDV operacional. Marca
    status_geolocalizacao=manual_edit e propaga a correção para o cache
    compartilhado `enderecos_cache` (public) pela chave canônica.

    O DatabaseWriter(schema="operacional") roda com search_path
    operacional, public — `pdvs` resolve em operacional.pdvs e
    `enderecos_cache` em public."""
    user = request.state.user
    tenant_id = user["tenant_id"]

    if user["role"] not in (
        "sales_router_adm",
        "tenant_adm",
        "tenant_operacional",
    ):
        raise HTTPException(status_code=403, detail="Sem permissão para editar.")

    if payload.pdv_lat is None or payload.pdv_lon is None:
        raise HTTPException(
            status_code=400, detail="pdv_lat e pdv_lon são obrigatórios."
        )

    if coordenada_generica(payload.pdv_lat, payload.pdv_lon):
        raise HTTPException(status_code=400, detail="Coordenadas inválidas.")

    writer = DatabaseWriter(schema="operacional")

    cache_key = writer.buscar_cache_key_pdv(pdv_id=pdv_id, tenant_id=tenant_id)
    if not cache_key:
        raise HTTPException(
            status_code=500,
            detail="PDV sem endereco_cache_key. Reprocessar carregamento.",
        )

    ok_pdv = writer.atualizar_lat_lon_pdv(
        pdv_id=pdv_id,
        lat=payload.pdv_lat,
        lon=payload.pdv_lon,
        tenant_id=tenant_id,
    )
    if not ok_pdv:
        raise HTTPException(status_code=404, detail="PDV não encontrado.")

    ok_cache = writer.atualizar_cache_por_chave(
        cache_key=cache_key,
        nova_lat=payload.pdv_lat,
        nova_lon=payload.pdv_lon,
    )
    if not ok_cache:
        logger.warning(
            f"[OPERACIONAL] cache não atualizado para PDV {pdv_id} "
            f"(cache_key={cache_key})"
        )

    return {"status": "success"}


# ============================================================
# 🗑️ Excluir um PDV operacional
# ============================================================
@router.delete(
    "/operacional/locais/{pdv_id}",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def excluir_pdv_operacional(request: Request, pdv_id: int):
    request.state.user  # garante autenticação
    tenant_id = request.state.user["tenant_id"]
    writer = DatabaseWriter(schema="operacional")
    ok = writer.excluir_pdv(pdv_id, tenant_id)
    if not ok:
        raise HTTPException(status_code=404, detail="PDV não encontrado.")
    return {"status": "ok", "message": "PDV excluído com sucesso.", "id": pdv_id}


# ============================================================
# 📍 PDVs de um carregamento operacional (paginado + filtros)
# ============================================================
@router.get(
    "/operacional/processamentos/{input_id}/pdvs",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def listar_pdvs_carregamento_operacional(
    request: Request,
    input_id: str,
    cnpj: str = Query(None),
    cidade: str = Query(None),
    uf: str = Query(None),
    bairro: str = Query(None),
    logradouro: str = Query(None),
    cep: str = Query(None),
    status_geolocalizacao: str = Query(None),
    limit: int = Query(1000, ge=1, le=1000),
    offset: int = Query(0, ge=0),
):
    tenant_id = request.state.user["tenant_id"]
    input_id = _normalizar_input_id(input_id)

    filtros = ["tenant_id = %s", "input_id = %s"]
    params: list = [tenant_id, input_id]
    if cnpj:
        filtros.append("cnpj = %s")
        params.append(re.sub(r"[^0-9]", "", cnpj))
    if cidade:
        filtros.append("cidade LIKE %s")
        params.append(f"%{normalize_text(cidade)}%")
    if uf:
        filtros.append("uf = %s")
        params.append(uf.strip().upper())
    if bairro:
        filtros.append("bairro LIKE %s")
        params.append(f"%{normalize_text(bairro)}%")
    if logradouro:
        filtros.append("logradouro LIKE %s")
        params.append(f"%{normalize_text(logradouro)}%")
    if cep:
        filtros.append("cep = %s")
        params.append(re.sub(r"[^0-9]", "", cep))
    if status_geolocalizacao:
        filtros.append("status_geolocalizacao = %s")
        params.append(status_geolocalizacao.strip())
    where_clause = " AND ".join(filtros)

    conn = _conn_operacional()
    try:
        total_df = pd.read_sql_query(
            f"SELECT COUNT(*) AS total FROM pdvs WHERE {where_clause};",
            conn,
            params=tuple(params),
        )
        total = int(total_df.iloc[0]["total"]) if not total_df.empty else 0
        df = pd.read_sql_query(
            f"""
            SELECT id, tenant_id, input_id, descricao,
                   cnpj, logradouro, numero, bairro, cidade, uf, cep,
                   pdv_lat, pdv_lon, pdv_endereco_completo,
                   status_geolocalizacao, pdv_vendas,
                   razao_social, nome_fantasia,
                   criado_em, atualizado_em
            FROM pdvs
            WHERE {where_clause}
            ORDER BY cidade, bairro, logradouro
            LIMIT %s OFFSET %s;
            """,
            conn,
            params=tuple(params) + (limit, offset),
        )
        manuais_df = pd.read_sql_query(
            """
            SELECT COUNT(*) AS manuais FROM pdvs
            WHERE tenant_id = %s AND input_id = %s
              AND status_geolocalizacao = 'manual_insert';
            """,
            conn,
            params=(tenant_id, input_id),
        )
        manuais = int(manuais_df.iloc[0]["manuais"]) if not manuais_df.empty else 0
    finally:
        conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})
    return {
        "total": total,
        "manuais": manuais,
        "limit": limit,
        "offset": offset,
        "pdvs": df.to_dict(orient="records"),
    }


# ============================================================
# 🔗 Dependências downstream de um carregamento operacional
# ============================================================
@router.get(
    "/operacional/processamentos/{input_id}/dependencies",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def listar_dependencias_operacional(request: Request, input_id: str):
    tenant_id = request.state.user["tenant_id"]
    input_id = _normalizar_input_id(input_id)

    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id FROM cluster_run WHERE tenant_id = %s AND input_id = %s",
                (tenant_id, input_id),
            )
            cluster_run_ids = [r[0] for r in cur.fetchall()]
            routings = subclusters = vendedores = 0
            if cluster_run_ids:
                cur.execute(
                    """
                    SELECT COUNT(DISTINCT routing_id), COUNT(*),
                           COUNT(DISTINCT assign_id) FILTER (WHERE assign_id IS NOT NULL)
                    FROM sales_subcluster
                    WHERE tenant_id = %s AND run_id = ANY(%s)
                    """,
                    (tenant_id, cluster_run_ids),
                )
                row = cur.fetchone()
                routings, subclusters, vendedores = (
                    row[0] or 0,
                    row[1] or 0,
                    row[2] or 0,
                )
    finally:
        conn.close()

    return {
        "tenant_id": tenant_id,
        "input_id": input_id,
        "setorizacoes": len(cluster_run_ids),
        "roteirizacoes": routings,
        "subclusters": subclusters,
        "consultores_alocados": vendedores,
        "tem_dependencias": len(cluster_run_ids) > 0,
    }


# ============================================================
# ❌ Excluir um carregamento operacional
# ============================================================
@router.delete(
    "/operacional/processamentos/{input_id}",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def excluir_processamento_operacional(
    request: Request,
    input_id: str,
    cascade: bool = Query(False),
):
    user = request.state.user
    tenant_id = user["tenant_id"]
    role = user.get("role")
    writer = DatabaseWriter(schema="operacional")

    if cascade:
        if role != "sales_router_adm":
            raise HTTPException(
                status_code=403,
                detail="Exclusão em cascata requer permissão de sales_router_adm.",
            )
        try:
            contagens = writer.excluir_processamento_cascata(
                tenant_id=tenant_id, input_id=input_id
            )
        except Exception as e:
            raise HTTPException(500, detail=f"Falha na exclusão em cascata: {e}")
        return {
            "status": "success",
            "mode": "cascade",
            "tenant_id": tenant_id,
            "input_id": input_id,
            "deleted": contagens,
        }

    if role not in ["sales_router_adm", "tenant_adm"]:
        raise HTTPException(status_code=403, detail="Sem permissão.")

    sucesso = writer.excluir_processamento_por_input(
        tenant_id=tenant_id, input_id=input_id
    )
    if not sucesso:
        raise HTTPException(
            status_code=400,
            detail=(
                "Carregamento vinculado a setorização/roteirização. "
                "Use ?cascade=true para excluir tudo (requer sales_router_adm)."
            ),
        )
    return {
        "status": "success",
        "mode": "default",
        "tenant_id": tenant_id,
        "input_id": input_id,
    }


# ============================================================
# 📥 Download XLSX de PDVs válidos (operacional)
# ============================================================
@router.get(
    "/operacional/processamentos/{input_id}/download-validos",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def download_validos_operacional(request: Request, input_id: str):
    tenant_id = request.state.user["tenant_id"]
    input_id = _normalizar_input_id(input_id)

    conn = _conn_operacional()
    try:
        df = pd.read_sql_query(
            """
            SELECT cnpj, logradouro, numero, bairro, cidade, uf, cep,
                   pdv_vendas, pdv_lat, pdv_lon,
                   status_geolocalizacao, pdv_endereco_completo,
                   descricao, criado_em, atualizado_em
            FROM pdvs
            WHERE tenant_id = %s AND input_id = %s
            ORDER BY cidade, bairro, logradouro;
            """,
            conn,
            params=(tenant_id, input_id),
        )
    finally:
        conn.close()

    if df.empty:
        raise HTTPException(404, "Nenhum PDV válido encontrado para este carregamento.")

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as xls:
        df.to_excel(xls, index=False, sheet_name="validos")
    buffer.seek(0)
    filename = f"pdvs_validos_{input_id}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ============================================================
# 📥 Download XLSX de inválidos (operacional.pdv_invalidos)
# ============================================================
def _gerar_xlsx_invalidos_operacional(tenant_id: int, input_id: str):
    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT cnpj, razao_social, nome_fantasia,
                       logradouro, numero, bairro, cidade, uf, cep,
                       pdv_vendas,
                       janela_atendimento_inicio, janela_atendimento_fim,
                       tempo_atendimento_min, is_estrategico,
                       pdv_lat, pdv_lon, motivo_invalidade
                FROM pdv_invalidos
                WHERE tenant_id = %s AND input_id = %s
                ORDER BY id
                """,
                (tenant_id, input_id),
            )
            rows = cur.fetchall()
    finally:
        conn.close()

    if not rows:
        raise HTTPException(404, "Nenhum registro inválido para este carregamento.")

    df = pd.DataFrame(
        rows,
        columns=[
            "cnpj", "razao_social", "nome_fantasia", "logradouro", "numero",
            "bairro", "cidade", "uf", "cep", "pdv_vendas",
            "janela_atendimento_inicio", "janela_atendimento_fim",
            "tempo_atendimento_min", "is_estrategico",
            "pdv_lat", "pdv_lon", "motivo_invalidade",
        ],
    )
    df = enrich_invalidos_for_export(df)
    buffer = io.BytesIO()
    df.to_excel(buffer, index=False, engine="openpyxl")
    buffer.seek(0)
    filename = f"pdvs_invalidos_{input_id}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get(
    "/operacional/jobs/{job_id}/download-invalidos",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def download_invalidos_job_operacional(request: Request, job_id: str):
    tenant_id = request.state.user["tenant_id"]
    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT input_id FROM historico_pdv_jobs "
                "WHERE tenant_id = %s AND job_id = %s LIMIT 1",
                (tenant_id, job_id),
            )
            row = cur.fetchone()
    finally:
        conn.close()
    if not row or not row[0]:
        raise HTTPException(404, "Job não encontrado ou sem input_id associado.")
    return _gerar_xlsx_invalidos_operacional(tenant_id, str(row[0]))


@router.get(
    "/operacional/processamentos/{input_id}/download-invalidos",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def download_invalidos_input_operacional(request: Request, input_id: str):
    tenant_id = request.state.user["tenant_id"]
    return _gerar_xlsx_invalidos_operacional(tenant_id, _normalizar_input_id(input_id))


# ============================================================
# 🧰 Descrição de um carregamento operacional
# ============================================================
def _carregamento_descricao_operacional(tenant_id: int, input_id: str):
    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT descricao FROM historico_pdv_jobs
                WHERE tenant_id = %s AND input_id = %s AND descricao IS NOT NULL
                ORDER BY criado_em DESC LIMIT 1;
                """,
                (tenant_id, input_id),
            )
            row = cur.fetchone()
            return row[0] if row else None
    finally:
        conn.close()


# ============================================================
# ➕ Adicionar PDVs ao carregamento operacional — via cadastro
# ============================================================
@router.post(
    "/operacional/processamentos/{input_id}/pdvs-from-cadastro",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def inserir_pdvs_from_cadastro_operacional(
    request: Request,
    input_id: str,
    payload: PdvsFromCadastroSchema,
):
    user = request.state.user
    tenant_id = user["tenant_id"]
    if user.get("role") not in (
        "sales_router_adm",
        "tenant_adm",
        "tenant_operacional",
    ):
        raise HTTPException(status_code=403, detail="Usuário sem permissão.")

    input_id = _normalizar_input_id(input_id)
    descricao = _carregamento_descricao_operacional(tenant_id, input_id)
    if descricao is None:
        raise HTTPException(
            status_code=404, detail="Carregamento não encontrado para este tenant."
        )

    ids = [str(i).strip() for i in (payload.cliente_ids or []) if str(i).strip()]
    if not ids:
        raise HTTPException(status_code=400, detail="Nenhum cliente selecionado.")

    conn = _conn_operacional()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT cnpj FROM pdvs WHERE tenant_id = %s AND input_id = %s",
                (tenant_id, input_id),
            )
            cnpjs_existentes = {r[0] for r in cur.fetchall()}

            cur.execute(
                """
                SELECT cnpj, logradouro, numero, bairro, cidade, uf, cep,
                       pdv_lat, pdv_lon, pdv_vendas,
                       janela_atendimento_inicio, janela_atendimento_fim,
                       tempo_atendimento_min, is_estrategico,
                       razao_social, nome_fantasia
                FROM cadastro_pdvs
                WHERE tenant_id = %s
                  AND id = ANY(%s::uuid[])
                  AND ativo = TRUE
                  AND pdv_lat IS NOT NULL
                  AND pdv_lon IS NOT NULL
                """,
                (tenant_id, ids),
            )
            rows = cur.fetchall()
            if not rows:
                raise HTTPException(
                    status_code=400,
                    detail="Nenhum cliente ativo e geocodificado entre os selecionados.",
                )

            valores = []
            ignorados = 0
            cnpjs_no_lote: set = set()
            for r in rows:
                (cnpj, log, num, bairro, cid, uf_, cep, lat, lon, vendas,
                 jini, jfim, tempo, estrat, rs, nf) = r
                if cnpj in cnpjs_existentes or cnpj in cnpjs_no_lote:
                    ignorados += 1
                    continue
                cnpjs_no_lote.add(cnpj)
                canonico, chave = _cache_keys_geocoding(log, num, cid, uf_)
                valores.append((
                    tenant_id, input_id, descricao, cnpj,
                    log, num, bairro, cid, uf_, cep,
                    canonico, chave, lat, lon, "cadastro_insert", vendas,
                    jini, jfim, tempo, estrat, rs, nf,
                ))

            inseridos = 0
            if valores:
                execute_values(
                    cur,
                    """
                    INSERT INTO pdvs (
                        tenant_id, input_id, descricao, cnpj,
                        logradouro, numero, bairro, cidade, uf, cep,
                        pdv_endereco_completo, endereco_cache_key,
                        pdv_lat, pdv_lon, status_geolocalizacao, pdv_vendas,
                        janela_atendimento_inicio, janela_atendimento_fim,
                        tempo_atendimento_min, is_estrategico,
                        razao_social, nome_fantasia
                    ) VALUES %s
                    ON CONFLICT (tenant_id, input_id, cnpj) DO NOTHING
                    """,
                    valores,
                )
                inseridos = len(valores)

        conn.commit()
        logger.info(
            f"[OPERACIONAL][PDVS_FROM_CADASTRO] input_id={input_id} "
            f"inseridos={inseridos} ignorados={ignorados}"
        )
        return {
            "status": "success",
            "input_id": input_id,
            "inseridos": inseridos,
            "ignorados_duplicados": ignorados,
        }
    except HTTPException:
        conn.rollback()
        raise
    except Exception as e:
        conn.rollback()
        logger.error(f"[OPERACIONAL][PDVS_FROM_CADASTRO][ERRO] {e}", exc_info=True)
        raise HTTPException(
            status_code=500, detail=f"Erro ao inserir PDVs do cadastro: {e}"
        )
    finally:
        conn.close()


# ============================================================
# ➕ Adicionar PDVs ao carregamento operacional — via XLSX
# (planilha já com lat/lon — sem geocodificação)
# ============================================================
@router.post(
    "/operacional/processamentos/{input_id}/pdvs-manuais",
    dependencies=[Depends(verify_token)],
    tags=["Operacional"],
)
def inserir_pdvs_manuais_operacional(
    request: Request,
    input_id: str,
    file: UploadFile = File(...),
):
    user = request.state.user
    tenant_id = user["tenant_id"]
    if user.get("role") not in (
        "sales_router_adm",
        "tenant_adm",
        "tenant_operacional",
    ):
        raise HTTPException(status_code=403, detail="Usuário sem permissão.")

    input_id = _normalizar_input_id(input_id)
    descricao = _carregamento_descricao_operacional(tenant_id, input_id)
    if descricao is None:
        raise HTTPException(
            status_code=404, detail="Carregamento não encontrado para este tenant."
        )

    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in (".xlsx", ".xls"):
        raise HTTPException(status_code=400, detail="Formato inválido. Envie XLSX.")

    try:
        conteudo = file.file.read()
        df = pd.read_excel(
            io.BytesIO(conteudo), dtype=str, engine="openpyxl"
        ).fillna("")
    except Exception as e:
        logger.error(f"[OPERACIONAL] erro ao ler planilha manual: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Falha ao ler a planilha: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Planilha vazia.")

    df.columns = [_normalizar_nome_coluna(c) for c in df.columns]
    obrigatorias = [
        "cnpj", "logradouro", "numero", "bairro",
        "cidade", "uf", "cep", "pdv_lat", "pdv_lon",
    ]
    faltantes = [c for c in obrigatorias if c not in df.columns]
    if faltantes:
        raise HTTPException(
            status_code=400,
            detail=f"Colunas ausentes na planilha: {', '.join(faltantes)}",
        )

    tem_vendas = "pdv_vendas" in df.columns
    tem_jan_ini = "janela_atendimento_inicio" in df.columns
    tem_jan_fim = "janela_atendimento_fim" in df.columns
    tem_t_atend = "tempo_atendimento_min" in df.columns
    tem_estrat = "is_estrategico" in df.columns
    tem_razao = "razao_social" in df.columns
    tem_fantasia = "nome_fantasia" in df.columns

    def _parse_coord(valor):
        texto = str(valor or "").strip().replace(",", ".")
        if not texto:
            return None
        try:
            numero = float(texto)
        except Exception:
            return None
        return numero if np.isfinite(numero) else None

    def _parse_vendas(valor):
        texto = str(valor or "").strip()
        if not texto:
            return None
        texto = texto.replace("R$", "").replace("r$", "").strip()
        if "," in texto:
            texto = texto.replace(".", "").replace(",", ".")
        texto = re.sub(r"[^0-9.]", "", texto)
        try:
            numero = float(texto)
        except Exception:
            return None
        return numero if np.isfinite(numero) and numero <= 1_000_000_000 else None

    def _parse_horario_min(valor):
        if valor is None:
            return None
        v = str(valor).strip()
        if not v or v.lower() in {"nan", "none", "null"}:
            return None
        m = re.match(r"^(\d{1,2}):(\d{2})(?::\d{2}(?:\.\d+)?)?$", v)
        if m:
            h, mm = int(m.group(1)), int(m.group(2))
            return h * 60 + mm if 0 <= h < 24 and 0 <= mm < 60 else None
        try:
            n = int(float(v.replace(",", ".")))
            if 0 <= n < 1440:
                return n
        except Exception:
            pass
        return None

    def _parse_tempo_atendimento(valor):
        if valor is None:
            return None
        v = str(valor).strip().replace("R$", "").replace(",", ".")
        if not v or v.lower() in {"nan", "none", "null"}:
            return None
        try:
            n = float(v)
            return n if 0 < n <= 1440 else None
        except Exception:
            return None

    _VERDADEIROS = {
        "true", "verdadeiro", "1", "sim", "s", "yes", "y", "x",
        "✓", "estrategico", "estrategica",
    }
    _FALSOS = {"false", "falso", "0", "nao", "n", "no"}

    def _parse_bool_estrategico(valor):
        if valor is None:
            return None
        if isinstance(valor, bool):
            return valor
        v = str(valor).strip().lower()
        if not v or v in {"nan", "none", "null"}:
            return None
        if v in _VERDADEIROS:
            return True
        if v in _FALSOS:
            return False
        return None

    reader = DatabaseReader(schema="operacional")
    cnpjs_existentes = set(reader.buscar_cnpjs_existentes(tenant_id, input_id))

    pdvs_para_inserir: list = []
    cache_updates: list = []
    cnpjs_no_lote: set = set()
    invalidos: list = []
    duplicados: list = []
    total_linhas = len(df)

    for posicao, (_, row) in enumerate(df.iterrows(), start=2):
        cnpj = re.sub(r"[^0-9]", "", str(row.get("cnpj", "") or ""))
        logradouro = str(row.get("logradouro", "") or "").strip()
        numero = str(row.get("numero", "") or "").strip()
        bairro = str(row.get("bairro", "") or "").strip()
        cidade = str(row.get("cidade", "") or "").strip().upper()
        uf = str(row.get("uf", "") or "").strip().upper()
        cep = re.sub(r"[^0-9]", "", str(row.get("cep", "") or ""))
        lat = _parse_coord(row.get("pdv_lat"))
        lon = _parse_coord(row.get("pdv_lon"))
        vendas = _parse_vendas(row.get("pdv_vendas")) if tem_vendas else None

        motivo = None
        if not cnpj:
            motivo = "cnpj_invalido"
        elif not logradouro:
            motivo = "logradouro_invalido"
        elif not cidade:
            motivo = "cidade_invalida"
        elif len(uf) != 2:
            motivo = "uf_invalida"
        elif lat is None or lon is None:
            motivo = "coordenadas_invalidas"
        elif coordenada_generica(lat, lon):
            motivo = "coordenadas_genericas"
        if motivo:
            invalidos.append({"linha": posicao, "cnpj": cnpj, "motivo": motivo})
            continue

        endereco_canonico, endereco_normalizado = _cache_keys_geocoding(
            logradouro, numero, cidade, uf
        )
        cache_updates.append((endereco_canonico, endereco_normalizado, lat, lon))

        if cnpj in cnpjs_existentes or cnpj in cnpjs_no_lote:
            duplicados.append(cnpj)
            continue
        cnpjs_no_lote.add(cnpj)

        cep_fmt = f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else ""
        base_endereco = (
            f"{logradouro} {numero}, {bairro}, {cidade} - {uf}"
            if numero
            else f"{logradouro}, {bairro}, {cidade} - {uf}"
        )
        if cep_fmt:
            base_endereco = f"{base_endereco}, {cep_fmt}"
        endereco_completo = f"{base_endereco}, Brasil"

        jan_ini = _parse_horario_min(row.get("janela_atendimento_inicio")) if tem_jan_ini else None
        jan_fim = _parse_horario_min(row.get("janela_atendimento_fim")) if tem_jan_fim else None
        t_atend = _parse_tempo_atendimento(row.get("tempo_atendimento_min")) if tem_t_atend else None
        estrat = _parse_bool_estrategico(row.get("is_estrategico")) if tem_estrat else None
        razao = str(row.get("razao_social") or "").strip() if tem_razao else ""
        fantasia = str(row.get("nome_fantasia") or "").strip() if tem_fantasia else ""

        pdvs_para_inserir.append(
            PDV(
                cnpj=cnpj,
                logradouro=logradouro,
                numero=numero,
                bairro=bairro,
                cidade=cidade,
                uf=uf,
                cep=cep,
                pdv_vendas=vendas,
                input_id=input_id,
                descricao=descricao,
                pdv_endereco_completo=endereco_completo,
                endereco_cache_key=endereco_normalizado,
                pdv_lat=lat,
                pdv_lon=lon,
                status_geolocalizacao="manual_insert",
                tenant_id=tenant_id,
                razao_social=razao or None,
                nome_fantasia=fantasia or None,
                janela_atendimento_inicio=jan_ini,
                janela_atendimento_fim=jan_fim,
                tempo_atendimento_min=t_atend,
                is_estrategico=estrat,
            )
        )

    writer = DatabaseWriter(schema="operacional")
    inseridos = 0
    if pdvs_para_inserir:
        inseridos = writer.inserir_pdvs(pdvs_para_inserir) or 0

    # Atualiza enderecos_cache (compartilhado em public).
    for endereco_canonico, endereco_normalizado, lat, lon in cache_updates:
        writer.salvar_cache_geocoding(
            endereco_canonico,
            endereco_normalizado,
            lat,
            lon,
            origem="manual_insert",
        )

    logger.info(
        f"[OPERACIONAL][PDVS_MANUAIS] input_id={input_id} inseridos={inseridos} "
        f"duplicados={len(duplicados)} invalidos={len(invalidos)}"
    )
    return {
        "status": "success",
        "input_id": input_id,
        "total_linhas": total_linhas,
        "inseridos": inseridos,
        "ignorados_duplicados": len(duplicados),
        "invalidos": len(invalidos),
        "detalhes_duplicados": duplicados[:50],
        "detalhes_invalidos": invalidos[:50],
    }

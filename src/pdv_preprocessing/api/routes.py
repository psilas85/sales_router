#sales_router/src/pdv_preprocessing/api/routes.py

# ==========================================================
# 📦 Imports — BLOCO ÚNICO (OBRIGATÓRIO)
# ==========================================================

# FastAPI
from fastapi import (
    APIRouter,
    HTTPException,
    Query,
    Depends,
    Request,
    UploadFile,
    Body,
    File,
)
from fastapi.responses import StreamingResponse

# Pydantic
from pydantic import BaseModel

# Logging
from loguru import logger

# Banco de dados
from database.db_connection import get_connection
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter

# Entidades e domínio
from pdv_preprocessing.entities.pdv_entity import PDV
from pdv_preprocessing.domain.address_normalizer import normalize_for_cache
from pdv_preprocessing.domain.utils_geo import coordenada_generica

# Jobs / filas
from pdv_preprocessing.pdv_jobs import processar_pdv
from redis import Redis
from rq import Queue
from rq.job import Job

from pathlib import Path

# Autenticação
from .dependencies import verify_token

# Utils padrão
from datetime import datetime, timedelta
from uuid import UUID, uuid4
import pandas as pd
import numpy as np
import unicodedata
import shutil
import os
import re
import io

from pdv_preprocessing.utils.file_utils import enrich_invalidos_for_export
import io


router = APIRouter()


PDV_PROGRESS_DEBUG = os.getenv("PDV_PROGRESS_DEBUG", "false").lower() == "true"


def parse_geocoding_metrics(message: str | None):
    if not message or not str(message).startswith("geocoding="):
        return None

    metrics = {}

    try:
        raw_payload = str(message).split("=", 1)[1]
        for chunk in raw_payload.split("|"):
            if ":" not in chunk:
                continue
            key, value = chunk.split(":", 1)
            key = key.strip()
            value = value.strip()
            if not key:
                continue
            metrics[key] = int(value) if value.isdigit() else value
    except Exception:
        return None

    return metrics or None


def enrich_job_history_rows(jobs: list[dict]):
    enriched = []

    for job in jobs:
        item = dict(job)
        item["integration_metrics"] = parse_geocoding_metrics(item.get("mensagem"))
        enriched.append(item)

    return enriched


def enrich_jobs_with_live_counts(jobs: list[dict], tenant_id: int):
    """
    Recalcula os números do card a partir do estado atual da tabela pdvs:
    - validos      = total de PDVs do input
    - manuais      = PDVs adicionados manualmente (status_geolocalizacao=manual_insert)
    - total_processados = validos + invalidos (invalidos vem do histórico original)
    Mantém o card sempre coerente com adições/exclusões manuais.
    """
    if not jobs:
        return jobs

    input_ids = [str(j["input_id"]) for j in jobs if j.get("input_id")]
    if not input_ids:
        for job in jobs:
            job["manuais"] = 0
        return jobs

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT input_id::text,
                   COUNT(*) AS validos,
                   COUNT(*) FILTER (
                       WHERE status_geolocalizacao = 'manual_insert'
                   ) AS manuais
            FROM pdvs
            WHERE tenant_id = %s
              AND input_id::text = ANY(%s)
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

def normalize_text(value: str | None):
    if not value:
        return value
    return (
        unicodedata.normalize("NFD", value)
        .encode("ascii", "ignore")
        .decode("utf-8")
        .upper()
        .strip()
    )


# ==========================================================
# 🧠 Health check (sem autenticação)
# ==========================================================
@router.get("/health", tags=["Status"])
def health_check():
    return {"status": "ok", "message": "PDV Preprocessing API saudável 🧩"}


# ==========================================================
# 🔍 Buscar PDV por CNPJ (autenticado)
# ==========================================================
@router.get("/buscar", dependencies=[Depends(verify_token)], tags=["PDVs"])
def buscar_pdv(request: Request, cnpj: str = Query(...)):
    user = request.state.user
    tenant_id = user["tenant_id"]

    conn = get_connection()
    reader = DatabaseReader(conn)
    pdv = reader.buscar_pdv_por_cnpj(tenant_id, cnpj)
    conn.close()

    if not pdv:
        raise HTTPException(status_code=404, detail="PDV não encontrado.")

    # 🧹 Sanitiza dados do PDV (caso contenha floats inválidos)
    for k, v in pdv.items():
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            pdv[k] = None

    logger.info(f"🔎 Tenant {tenant_id} ({user['role']}) consultou PDV {cnpj}")
    return {"pdv": pdv, "usuario": user}


# ==========================================================
# 📋 Listar PDVs (autenticado)
# ==========================================================
@router.get("/listar", dependencies=[Depends(verify_token)], tags=["PDVs"])
def listar_pdvs(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]

    conn = get_connection()
    reader = DatabaseReader(conn)
    df = reader.listar_pdvs_por_tenant(tenant_id)
    conn.close()

    if df.empty:
        return {"usuario": user, "total": 0, "pdvs": []}

    # 🧹 Sanitiza DataFrame completamente antes de serializar
    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})

    logger.info(f"📄 Tenant {tenant_id} ({user['role']}) listou {len(df)} PDVs")

    # 🧹 Sanitiza antes de converter para JSON
    df = df.replace({float("inf"): None, float("-inf"): None})
    df = df.where(pd.notnull(df), None)

    return {
        "usuario": user,
        "total": int(len(df)),
        "pdvs": df.to_dict(orient="records"),
    }


# ==========================================================
# ✏️ Atualizar PDV (autenticado + controle de role)
# ==========================================================

@router.put("/atualizar", dependencies=[Depends(verify_token)], tags=["PDVs"])
def atualizar_pdv(
    request: Request,
    cnpj: str = Query(...),
    logradouro: str | None = Query(None),
    numero: str | None = Query(None),
    bairro: str | None = Query(None),
    cidade: str | None = Query(None),
    uf: str | None = Query(None),
    cep: str | None = Query(None),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    # 🔐 Permissão
    if user.get("role") not in [
        "sales_router_adm",
        "tenant_adm",
        "tenant_operacional",
    ]:
        raise HTTPException(status_code=403, detail="Usuário sem permissão.")

    conn = get_connection()
    reader = DatabaseReader(conn)
    writer = DatabaseWriter()

    existente = reader.buscar_pdv_por_cnpj(tenant_id, cnpj)
    if not existente:
        conn.close()
        raise HTTPException(status_code=404, detail="PDV não encontrado.")

    atualizado = dict(existente)

    # 🔧 Atualiza APENAS campos textuais (se vierem preenchidos)
    for campo, valor in {
        "logradouro": logradouro,
        "numero": numero,
        "bairro": bairro,
        "cidade": cidade,
        "uf": uf,
        "cep": cep,
    }.items():
        if valor is not None and str(valor).strip():
            atualizado[campo] = normalize_text(valor)

    # 🧩 Recompõe endereço completo
    atualizado["pdv_endereco_completo"] = (
        f"{atualizado.get('logradouro','')}, {atualizado.get('numero','')}, "
        f"{atualizado.get('bairro','')}, {atualizado.get('cidade','')} - "
        f"{atualizado.get('uf','')}, {atualizado.get('cep','')}"
    ).strip()

    # ⏱️ Timestamp
    atualizado["atualizado_em"] = datetime.utcnow()

    # ❗ REGRAS EXPLÍCITAS
    # - NÃO altera pdv_lat / pdv_lon
    # - NÃO altera status_geolocalizacao
    # - NÃO atualiza cache

    pdv = PDV(**{**atualizado, "tenant_id": tenant_id})
    writer.atualizar_pdv_completo(pdv)

    conn.close()

    return {
        "status": "success",
        "pdv": atualizado,
    }



# ==========================================================
# 🚀 Enfileirar novo processamento de PDVs (upload CSV)
# ==========================================================

@router.post("/upload", dependencies=[Depends(verify_token)], tags=["Jobs"])
def upload_pdv(
    request: Request,
    arquivo: str = Query(..., description="Caminho do arquivo XLSX ou CSV dentro de /app/data"),
    descricao: str = Query(..., description="Descrição amigável do job"),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        conn_redis = Redis(host="redis", port=6379)
        queue = Queue("pdv_jobs", connection=conn_redis)

        job = queue.enqueue(
            processar_pdv,
            tenant_id,
            arquivo,
            descricao,
            job_timeout=36000
        )



        logger.info(f"🚀 Novo job enfileirado: {job.id} | tenant={tenant_id} | arquivo={arquivo}")
        return {
            "status": "queued",
            "job_id": job.id,
            "tenant_id": tenant_id,
            "arquivo": arquivo,
            "descricao": descricao,
        }
    except Exception as e:
        logger.error(f"❌ Erro ao enfileirar job: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================================
# ♻️ Reprocessar input_id existente
# ==========================================================
@router.post("/reprocessar", dependencies=[Depends(verify_token)], tags=["Jobs"])
def reprocessar_input(request: Request, input_id: str = Query(...), descricao: str = Query(...)):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            "SELECT DISTINCT arquivo FROM historico_pdv_jobs WHERE tenant_id=%s AND input_id=%s LIMIT 1;",
            (tenant_id, input_id),
        )
        row = cur.fetchone()
        conn.close()

        if not row:
            raise HTTPException(status_code=404, detail=f"Input ID {input_id} não encontrado.")

        arquivo = row[0]
        conn_redis = Redis(host="redis", port=6379)
        queue = Queue("pdv_jobs", connection=conn_redis)
        job = queue.enqueue(processar_pdv, tenant_id, arquivo, descricao)

        logger.info(f"♻️ Reprocessando input_id={input_id} | job={job.id}")
        return {
            "status": "queued",
            "job_id": job.id,
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao,
            "arquivo": arquivo,
        }
    except Exception as e:
        logger.error(f"❌ Erro ao reprocessar input_id {input_id}: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================================
# 📤 Upload direto de arquivo (multipart/form-data)
#     ➜ XLSX (padrão) ou CSV
# ==========================================================

@router.post("/upload-file", dependencies=[Depends(verify_token)], tags=["Jobs"])
def upload_arquivo(
    request: Request,
    descricao: str = Query(..., description="Descrição amigável do job"),
    file: UploadFile = File(...),
):
    """
    Recebe um arquivo XLSX ou CSV enviado pelo cliente (multipart/form-data),
    salva no volume /app/data e enfileira o processamento automaticamente.
    """
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        # --------------------------------------------------
        # 🔎 Validação de extensão
        # --------------------------------------------------
        ext = os.path.splitext(file.filename)[1].lower()
        if ext not in [".xlsx", ".xls", ".csv"]:
            raise HTTPException(
                status_code=400,
                detail="Formato inválido. Envie um arquivo XLSX ou CSV."
            )

        # --------------------------------------------------
        # 📁 Diretório destino
        # --------------------------------------------------
        base_dir = "/app/data"
        os.makedirs(base_dir, exist_ok=True)

        # --------------------------------------------------
        # 🕒 Nome único
        # --------------------------------------------------
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        nome_final = f"pdvs_{tenant_id}_{timestamp}_{file.filename}"
        caminho_final = os.path.join(base_dir, nome_final)

        # --------------------------------------------------
        # 💾 Salvar arquivo
        # --------------------------------------------------
        with open(caminho_final, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # --------------------------------------------------
        # 🚀 Enfileirar job
        # --------------------------------------------------
        conn_redis = Redis(host="redis", port=6379)
        queue = Queue("pdv_jobs", connection=conn_redis)

        job = queue.enqueue(
            processar_pdv,
            tenant_id,
            caminho_final,
            descricao,
            job_timeout=36000  # 10 horas
        )

        logger.info(f"📤 Arquivo salvo em {caminho_final}")
        logger.info(f"🚀 Job enfileirado: {job.id} | tenant={tenant_id}")

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
        logger.error(f"❌ Erro no upload multipart: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================================
# 🗺️ Mapas de PDVs — removidos em 2026-05-18.
# Os endpoints abaixo geravam HTMLs estáticos em /app/output/maps/ e
# foram substituídos por renderização inline no frontend, que busca os
# pontos via `/pdv/mapa-locais` e renderiza com Leaflet (componente
# `<GeocodeResultMap>`).
#
# Endpoints removidos:
#   POST  /pdv/gerar-mapa
#   GET   /pdv/mapa-status
#   POST  /pdv/mapas-status
#   GET   /pdv/download-mapa
#   GET   /pdv/ver-mapa
#
# Páginas que faziam essa visualização (Carregamentos e /dados/mapas/{id})
# foram refatoradas pra usar o mapa inline.
# ==========================================================

# ==========================================================
# ❌ Excluir processamento completo (por input_id)
# ==========================================================
@router.get(
    "/processamentos/{input_id}/dependencies",
    dependencies=[Depends(verify_token)],
    tags=["Jobs"],
)
def listar_dependencias(request: Request, input_id: str):
    """
    Retorna contagem de registros downstream vinculados a um input_id.
    Frontend usa pra mostrar preview no modal de exclusão (e decidir
    se mostra a opção de cascata).
    """
    user = request.state.user
    tenant_id = user["tenant_id"]
    input_id = _normalizar_input_id(input_id)

    conn = get_connection()
    try:
        cur = conn.cursor()

        # Tudo dentro do mesmo tenant (multi-tenant safety)
        cur.execute(
            """
            SELECT id, clusterization_id
            FROM cluster_run
            WHERE tenant_id = %s AND input_id = %s
            """,
            (tenant_id, input_id),
        )
        runs = cur.fetchall()
        cluster_run_ids = [r[0] for r in runs]

        contagem_routings = 0
        contagem_vendedores = 0
        contagem_subclusters = 0

        if cluster_run_ids:
            cur.execute(
                """
                SELECT
                    COUNT(DISTINCT routing_id) AS routings,
                    COUNT(*) AS subclusters,
                    COUNT(DISTINCT assign_id) FILTER (WHERE assign_id IS NOT NULL) AS assigns
                FROM sales_subcluster
                WHERE tenant_id = %s AND run_id = ANY(%s)
                """,
                (tenant_id, cluster_run_ids),
            )
            row = cur.fetchone()
            contagem_routings = row[0] or 0
            contagem_subclusters = row[1] or 0
            contagem_vendedores = row[2] or 0

        return {
            "tenant_id": tenant_id,
            "input_id": input_id,
            "setorizacoes": len(cluster_run_ids),
            "roteirizacoes": contagem_routings,
            "subclusters": contagem_subclusters,
            "consultores_alocados": contagem_vendedores,
            "tem_dependencias": len(cluster_run_ids) > 0,
        }
    finally:
        conn.close()


@router.delete(
    "/processamentos/{input_id}",
    dependencies=[Depends(verify_token)],
    tags=["Jobs"],
)
def excluir_processamento(
    request: Request,
    input_id: str,
    cascade: bool = Query(
        False,
        description=(
            "Se true, faz exclusão em CASCATA: setorizações, roteirizações, "
            "vendedores assignados, históricos derivados. "
            "Requer role sales_router_adm. Operação destrutiva e irreversível."
        ),
    ),
):
    user = request.state.user
    tenant_id = user["tenant_id"]
    role = user.get("role")

    writer = DatabaseWriter()

    # ----------------------------------------------------------
    # Modo CASCATA — destrutivo, só sales_router_adm
    # ----------------------------------------------------------
    if cascade:
        if role != "sales_router_adm":
            raise HTTPException(
                status_code=403,
                detail="Exclusão em cascata requer permissão de sales_router_adm.",
            )

        try:
            contagens = writer.excluir_processamento_cascata(
                tenant_id=tenant_id,
                input_id=input_id,
            )
        except Exception as e:
            raise HTTPException(
                status_code=500,
                detail=f"Falha na exclusão em cascata: {e}",
            )

        return {
            "status": "success",
            "mode": "cascade",
            "tenant_id": tenant_id,
            "input_id": input_id,
            "deleted": contagens,
            "message": "Processamento e dependências excluídos em cascata.",
        }

    # ----------------------------------------------------------
    # Modo padrão — só apaga se NÃO houver cluster_run vinculado
    # ----------------------------------------------------------
    if role not in ["sales_router_adm", "tenant_adm"]:
        raise HTTPException(status_code=403, detail="Sem permissão.")

    sucesso = writer.excluir_processamento_por_input(
        tenant_id=tenant_id,
        input_id=input_id,
    )

    if not sucesso:
        raise HTTPException(
            status_code=400,
            detail=(
                "Processamento vinculado a setorização/roteirização. "
                "Use ?cascade=true para excluir tudo (requer sales_router_adm)."
            ),
        )

    return {
        "status": "success",
        "mode": "default",
        "tenant_id": tenant_id,
        "input_id": input_id,
        "message": "Processamento excluído com sucesso.",
    }

# ==========================================================
# 📜 Listar últimos jobs de PDV (com paginação)
# ==========================================================

# Whitelist de colunas ordenáveis em /jobs/ultimos.
# Importante: ordem por chave estrangeira ao SQL — protege contra injection.
_SORTABLE_ULTIMOS_JOBS = {
    "criado_em": "criado_em",
    "descricao": "descricao",
    "total_processados": "total_processados",
    "validos": "validos",
    "invalidos": "invalidos",
}


@router.get("/jobs/ultimos", dependencies=[Depends(verify_token)], tags=["Jobs"])
def listar_ultimos_jobs(
    request: Request,
    limit: int = Query(10, ge=1, le=100),
    offset: int = Query(0, ge=0),
    sort_by: str = Query("criado_em"),
    sort_dir: str = Query("desc"),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    sort_col = _SORTABLE_ULTIMOS_JOBS.get(sort_by, "criado_em")
    sort_dir_sql = "ASC" if str(sort_dir).lower() == "asc" else "DESC"
    # criado_em DESC como tie-breaker garante ordem estável quando os
    # valores da coluna principal são iguais ou NULL.
    order_by_clause = f"{sort_col} {sort_dir_sql} NULLS LAST, criado_em DESC"

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        f"""
        SELECT
            id,
            tenant_id,
            job_id,
            arquivo,
            status,
            total_processados,
            validos,
            invalidos,
            arquivo_invalidos,
            mensagem,
            criado_em,
            inseridos,
            sobrescritos,
            descricao,
            input_id
        FROM (
            SELECT DISTINCT ON (input_id)
                *
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
        """
        SELECT COUNT(DISTINCT input_id)
        FROM historico_pdv_jobs
        WHERE tenant_id = %s
        """,
        (tenant_id,),
    )
    total = cur.fetchone()[0]

    colunas = [
        "id",
        "tenant_id",
        "job_id",
        "arquivo",
        "status",
        "total_processados",
        "validos",
        "invalidos",
        "arquivo_invalidos",
        "mensagem",
        "criado_em",
        "inseridos",
        "sobrescritos",
        "descricao",
        "input_id",
    ]

    jobs = [dict(zip(colunas, row)) for row in rows]
    jobs = enrich_job_history_rows(jobs)

    cur.close()
    conn.close()

    jobs = enrich_jobs_with_live_counts(jobs, tenant_id)

    return {
        "total": total,
        "jobs": jobs,
    }



# ==========================================================
# 📋 Filtrar jobs — aceita DD/MM/YYYY e YYYY-MM-DD
# ==========================================================


def parse_data(valor):
    if not valor:
        return None

    valor = valor.strip()

    # DD/MM/YYYY
    if "/" in valor:
        try:
            return datetime.strptime(valor, "%d/%m/%Y").date()
        except:
            raise HTTPException(status_code=400,
                                detail=f"Data inválida (esperado DD/MM/YYYY): {valor}")

    # YYYY-MM-DD
    if "-" in valor:
        try:
            return datetime.strptime(valor, "%Y-%m-%d").date()
        except:
            raise HTTPException(status_code=400,
                                detail=f"Data inválida (esperado YYYY-MM-DD): {valor}")

    raise HTTPException(status_code=400, detail=f"Formato de data inválido: {valor}")


@router.get("/jobs/filtrar", dependencies=[Depends(verify_token)], tags=["Jobs"])
def filtrar_jobs(
    request: Request,
    data_inicio: str = Query(None),
    data_fim: str = Query(None),
    descricao: str = Query(None),
    limit: int = Query(20, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    data_inicio_dt = parse_data(data_inicio)
    data_fim_dt = parse_data(data_fim)

    filtros = ["tenant_id = %s"]
    params = [tenant_id]
 

    if data_inicio_dt:
        filtros.append("criado_em >= %s")
        params.append(datetime.combine(data_inicio_dt, datetime.min.time()))

    if data_fim_dt:
        filtros.append("criado_em < %s")
        params.append(datetime.combine(data_fim_dt + timedelta(days=1), datetime.min.time()))


    if descricao:
        filtros.append("descricao ILIKE %s")
        params.append(f"%{descricao}%")

    where_clause = " AND ".join(filtros)

    # FILTRAR_JOBS_CAP: máximo de registros acessíveis via filtro (não é
    # o limit por página — é o teto total). Acima disso o front avisa
    # pra refinar, em vez de paginar histórico antigo via filtro.
    FILTRAR_JOBS_CAP = 100

    # COUNT real (input_ids distintos que casam com o filtro) — usado
    # pelo front pra decidir se exibe o aviso "refine o filtro".
    count_sql = f"""
        SELECT COUNT(*) FROM (
            SELECT DISTINCT input_id
            FROM historico_pdv_jobs
            WHERE {where_clause}
        ) t;
    """

    # Offset é truncado para não ultrapassar o cap (evita páginas vazias
    # além do registro 100º).
    safe_offset = max(0, min(offset, max(FILTRAR_JOBS_CAP - limit, 0)))

    sql = f"""
        SELECT *
        FROM (
            SELECT DISTINCT ON (input_id)
                tenant_id,
                job_id,
                input_id,
                descricao,
                arquivo,
                status,
                total_processados,
                validos,
                invalidos,
                mensagem,
                criado_em
            FROM historico_pdv_jobs
            WHERE {where_clause}
            ORDER BY input_id, criado_em DESC
        ) t
        ORDER BY criado_em DESC
        LIMIT %s OFFSET %s;
    """

    page_params = tuple(params) + (limit, safe_offset)

    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(count_sql, tuple(params))
        total = int(cur.fetchone()[0] or 0)
        cur.close()

        df = pd.read_sql_query(sql, conn, params=page_params)
    finally:
        conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})

    jobs = enrich_job_history_rows(df.to_dict(orient="records"))
    jobs = enrich_jobs_with_live_counts(jobs, tenant_id)

    return {
        "total": total,
        "jobs": jobs,
        "cap": FILTRAR_JOBS_CAP,
    }



# ==========================================================
# 📋 Listar jobs (AGORA ANTES DAS ROTAS DINÂMICAS)
# ==========================================================
@router.get("/jobs", dependencies=[Depends(verify_token)], tags=["Jobs"])
def listar_jobs(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]

    conn = get_connection()
    df = pd.read_sql_query(
        """
        SELECT *
        FROM (
            SELECT DISTINCT ON (input_id)
                id,
                tenant_id,
                job_id,
                input_id,
                descricao,
                arquivo,
                status,
                total_processados,
                validos,
                invalidos,
                arquivo_invalidos,
                mensagem,
                criado_em
            FROM historico_pdv_jobs
            WHERE tenant_id = %s
            ORDER BY input_id, criado_em DESC
        ) t
        ORDER BY criado_em DESC
        LIMIT 20;
        """,
        conn,
        params=(tenant_id,),
    )
    conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})

    jobs = enrich_job_history_rows(df.to_dict(orient="records"))
    jobs = enrich_jobs_with_live_counts(jobs, tenant_id)

    return {
        "total": int(len(df)),
        "jobs": jobs,
    }


# ============================================================================
# 📍 Gestão de Locais — LISTAR (GET /pdv/locais)
# ============================================================================

@router.get("/locais", dependencies=[Depends(verify_token)], tags=["Locais"])
def listar_locais(
    request: Request,
    input_id: str = Query(..., description="Input ID obrigatório"),
    uf: str = Query(..., description="UF obrigatória (SP, RJ etc.)"),
    cidade: str = Query(None),
    cnpj: str = Query(None),
    logradouro: str = Query(None),
    bairro: str = Query(None),
    cep: str = Query(None),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    # 🔧 FIX DEFINITIVO — normalização forte
    try:
        input_id_limpo = re.sub(r"[^a-fA-F0-9\-]", "", input_id)
        input_id = str(UUID(input_id_limpo))
    except Exception as e:
        logger.error(f"input_id inválido: repr={repr(input_id)} erro={e}")
        raise HTTPException(status_code=400, detail="input_id inválido")

    filtros = ["tenant_id = %s", "input_id = %s", "uf = %s"]
    params = [tenant_id, input_id, uf]

    if cidade:
        cidade_norm = normalize_text(cidade)
        filtros.append("cidade LIKE %s")
        params.append(f"%{cidade_norm}%")


    if cnpj:
        filtros.append("cnpj = %s")
        params.append(cnpj)

    if logradouro:
        filtros.append("logradouro LIKE %s")
        params.append(f"%{normalize_text(logradouro)}%")

    if bairro:
        filtros.append("bairro LIKE %s")
        params.append(f"%{normalize_text(bairro)}%")


    if cep:
        filtros.append("cep = %s")
        params.append(cep)

    sql = f"""
        SELECT 
            id, tenant_id, input_id, descricao,
            cnpj, logradouro, numero, bairro, cidade, uf, cep,
            pdv_lat, pdv_lon, pdv_endereco_completo,
            status_geolocalizacao, pdv_vendas,
            criado_em, atualizado_em
        FROM pdvs
        WHERE {" AND ".join(filtros)}
        ORDER BY cidade, bairro, logradouro
        LIMIT 500;
    """

    conn = get_connection()
    df = pd.read_sql_query(sql, conn, params=tuple(params))
    conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})

    return {
        "total": int(len(df)),
        "pdvs": df.to_dict(orient="records"),
    }


@router.get("/mapa-locais", dependencies=[Depends(verify_token)], tags=["Locais"])
def listar_locais_mapa(
    request: Request,
    input_id: str = Query(..., description="Input ID obrigatório"),
    limit: int = Query(1000, ge=1, le=1000),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        input_id_limpo = re.sub(r"[^a-fA-F0-9\-]", "", input_id)
        input_id = str(UUID(input_id_limpo))
    except Exception as e:
        logger.error(f"input_id inválido: repr={repr(input_id)} erro={e}")
        raise HTTPException(status_code=400, detail="input_id inválido")

    conn = get_connection()

    total_df = pd.read_sql_query(
        """
        SELECT COUNT(*) AS total
        FROM pdvs
        WHERE tenant_id = %s
          AND input_id = %s
          AND pdv_lat IS NOT NULL
          AND pdv_lon IS NOT NULL;
        """,
        conn,
        params=(tenant_id, input_id),
    )

    df = pd.read_sql_query(
        """
        SELECT
            id,
            cnpj,
            pdv_lat AS lat,
            pdv_lon AS lon,
            cidade,
            COALESCE(
                NULLIF(TRIM(pdv_endereco_completo), ''),
                CONCAT_WS(', ', logradouro, numero, bairro, cidade, uf, cep)
            ) AS endereco
        FROM pdvs
        WHERE tenant_id = %s
          AND input_id = %s
          AND pdv_lat IS NOT NULL
          AND pdv_lon IS NOT NULL
        ORDER BY RANDOM()
        LIMIT %s;
        """,
        conn,
        params=(tenant_id, input_id, limit),
    )

    conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})

    return {
        "total": int(total_df.iloc[0]["total"]) if not total_df.empty else 0,
        "pontos": df.to_dict(orient="records"),
    }


# ============================================================
# ✏️ Gestão de Locais — EDITAR COORDENADAS (FINAL)
# PUT /pdv/locais/{pdv_id}
#
# REGRAS:
# - ÚNICO endpoint que altera lat/lon
# - Sempre marca status_geolocalizacao = manual_edit
# - Sempre atualiza enderecos_cache pela chave canônica
# - NÃO altera endereço textual
# ============================================================



# --------------------------------------------------
# 📦 Payload
# --------------------------------------------------
class EditarLocalPayload(BaseModel):
    pdv_lat: float
    pdv_lon: float


# --------------------------------------------------
# ✏️ Endpoint
# --------------------------------------------------
@router.put(
    "/locais/{pdv_id}",
    dependencies=[Depends(verify_token)],
    tags=["Locais"],
)
def editar_local(
    request: Request,
    pdv_id: int,
    payload: EditarLocalPayload = Body(...),
):
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
            status_code=400,
            detail="pdv_lat e pdv_lon são obrigatórios."
        )

    if coordenada_generica(payload.pdv_lat, payload.pdv_lon):
        raise HTTPException(status_code=400, detail="Coordenadas inválidas.")

    writer = DatabaseWriter()

    cache_key = writer.buscar_cache_key_pdv(
        pdv_id=pdv_id,
        tenant_id=tenant_id,
    )

    if not cache_key:
        raise HTTPException(
            status_code=500,
            detail="PDV sem endereco_cache_key. Reprocessar input."
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
            f"⚠️ Cache não atualizado para PDV {pdv_id} (cache_key={cache_key})"
        )

    return {"status": "success"}




# ============================================================
# ❌ Excluir PDV (DELETE /pdv/locais/{pdv_id})
# ============================================================
@router.delete("/locais/{pdv_id}", tags=["PDVs"], dependencies=[Depends(verify_token)])
def excluir_pdv(request: Request, pdv_id: int):
    user = request.state.user
    tenant_id = user["tenant_id"]

    writer = DatabaseWriter()

    # executa exclusão com tenant_id
    ok = writer.excluir_pdv(pdv_id, tenant_id)

    if not ok:
        raise HTTPException(status_code=404, detail="PDV não encontrado.")

    return {
        "status": "ok",
        "message": "PDV excluído com sucesso.",
        "id": pdv_id
    }


# ==========================================================
# 🔍 Buscar detalhes de um job específico (BANCO → Redis fallback)
# GET /pdv/jobs/{job_id}
# ==========================================================
@router.get("/jobs/{job_id}", dependencies=[Depends(verify_token)], tags=["Jobs"])
def detalhar_job(request: Request, job_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]

    # --------------------------------------------------
    # 1️⃣ FONTE DA VERDADE: BANCO DE DADOS
    # --------------------------------------------------
    try:
        conn = get_connection()
        cur = conn.cursor()

        cur.execute(
            """
            SELECT
                job_id,
                status,
                descricao,
                arquivo,
                input_id,
                total_processados,
                validos,
                invalidos,
                inseridos,
                sobrescritos,
                arquivo_invalidos,
                mensagem,
                criado_em
            FROM historico_pdv_jobs
            WHERE tenant_id = %s
              AND job_id = %s
            LIMIT 1;
            """,
            (tenant_id, job_id),
        )

        row = cur.fetchone()
        conn.close()

        if row:
            colunas = [
                "job_id",
                "status",
                "descricao",
                "arquivo",
                "input_id",
                "total_processados",
                "validos",
                "invalidos",
                "inseridos",
                "sobrescritos",
                "arquivo_invalidos",
                "mensagem",
                "criado_em",
            ]
            return dict(zip(colunas, row))

    except Exception as e:
        logger.warning(f"⚠️ Erro ao buscar job no banco, tentando Redis: {e}")

    # --------------------------------------------------
    # 2️⃣ FALLBACK: REDIS (job ainda em execução)
    # --------------------------------------------------
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



# ==========================================================
# 📊 Consultar progresso em tempo real de um job
# ==========================================================


@router.get("/jobs/{job_id}/progress", dependencies=[Depends(verify_token)], tags=["Jobs"])
def progresso_job(request: Request, job_id: str):
    """
    Retorno padronizado para o frontend do SalesRouter,
    compatível com o comportamento do HubRouter.
    """
    try:
        conn_redis = Redis(host="redis", port=6379)
        job = Job.fetch(job_id, connection=conn_redis)

        meta = job.meta or {}
        progresso = meta.get("progress", 0)
        etapa = meta.get("step", "Processando...")
        job_status = job.get_status(refresh=True)

        def log_progress(payload_status: str, payload_progress: int, payload_step: str):
            if not PDV_PROGRESS_DEBUG:
                return

            logger.info(
                "[PDV_PROGRESS_DEBUG] "
                f"job_id={job_id} "
                f"rq_status={job_status} "
                f"payload_status={payload_status} "
                f"progress={payload_progress} "
                f"step={payload_step}"
            )

        # -------------------------
        # STATUS PADRONIZADO
        # -------------------------
        if job.is_finished:
            payload = {
                "job_id": job.id,
                "status": "done",
                "progress": 100,
                "step": "Finalizado"
            }
            log_progress(payload["status"], payload["progress"], payload["step"])
            return payload

        if job.is_failed:
            payload = {
                "job_id": job.id,
                "status": "error",
                "progress": progresso,
                "step": etapa
            }
            log_progress(payload["status"], payload["progress"], payload["step"])
            return payload

        # Qualquer outro estado → sempre RUNNING
        payload = {
            "job_id": job.id,
            "status": "running",
            "progress": progresso,
            "step": etapa
        }
        log_progress(payload["status"], payload["progress"], payload["step"])
        return payload

    except Exception as e:
        logger.error(f"❌ Erro ao consultar progresso do job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=404, detail=f"Job não encontrado ou expirado: {e}")


# ==========================================================
# 📥 Download XLSX de registros inválidos (on-demand do banco)
# ==========================================================
# Em vez de servir um XLSX pré-gerado em disco, busca os inválidos em
# pdv_invalidos e gera o XLSX em memória. Sem arquivos órfãos no host.

def _gerar_xlsx_invalidos_stream(
    tenant_id: int,
    input_id: str,
) -> StreamingResponse:
    """Constrói e retorna um StreamingResponse de XLSX a partir do banco.

    Ordem das colunas segue o template de upload + extras de inválido
    (motivo_invalidade/label) + lat/lon, pra o usuário poder ajustar
    o registro e reuploadar manualmente (ou usar como base p/ inserção
    geocodificada manualmente).
    """
    # Colunas opcionais (razao_social, nome_fantasia, janela_*, lat/lon)
    # foram adicionadas via lazy migration em salvar_invalidos_batch;
    # registros antigos retornam NULL nelas, o que o openpyxl exporta
    # como célula vazia (comportamento desejado p/ planilha de upload).
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT cnpj, razao_social, nome_fantasia,
                   logradouro, numero, bairro, cidade, uf, cep,
                   pdv_vendas,
                   janela_atendimento_inicio, janela_atendimento_fim,
                   tempo_atendimento_min, is_estrategico,
                   pdv_lat, pdv_lon,
                   motivo_invalidade
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
        raise HTTPException(
            status_code=404,
            detail="Nenhum registro inválido para este carregamento.",
        )

    df = pd.DataFrame(
        rows,
        columns=[
            "cnpj",
            "razao_social",
            "nome_fantasia",
            "logradouro",
            "numero",
            "bairro",
            "cidade",
            "uf",
            "cep",
            "pdv_vendas",
            "janela_atendimento_inicio",
            "janela_atendimento_fim",
            "tempo_atendimento_min",
            "is_estrategico",
            "pdv_lat",
            "pdv_lon",
            "motivo_invalidade",
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
    "/jobs/{job_id}/download-invalidos",
    dependencies=[Depends(verify_token)],
    tags=["Jobs"],
)
def download_invalidos(request: Request, job_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]

    # Resolve input_id a partir do job_id no histórico
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT input_id
            FROM historico_pdv_jobs
            WHERE tenant_id = %s AND job_id = %s
            LIMIT 1
            """,
            (tenant_id, job_id),
        )
        row = cur.fetchone()
    finally:
        conn.close()

    if not row or not row[0]:
        raise HTTPException(
            status_code=404,
            detail="Job não encontrado ou sem input_id associado.",
        )

    return _gerar_xlsx_invalidos_stream(tenant_id, str(row[0]))


# ==========================================================
# 🧰 Helpers — gestão de carregamento (input_id)
# ==========================================================

def _normalizar_input_id(input_id: str) -> str:
    try:
        input_id_limpo = re.sub(r"[^a-fA-F0-9\-]", "", input_id or "")
        return str(UUID(input_id_limpo))
    except Exception as e:
        logger.error(f"input_id inválido: repr={repr(input_id)} erro={e}")
        raise HTTPException(status_code=400, detail="input_id inválido")


def _normalizar_nome_coluna(coluna: str) -> str:
    coluna = str(coluna or "").strip().lower()
    coluna = unicodedata.normalize("NFKD", coluna)
    return "".join(ch for ch in coluna if not unicodedata.combining(ch))


def _normalizar_prefixo_logradouro(logradouro: str) -> str:
    """Replica geocoding_engine.domain.cache_key_builder._normalize_street_prefix."""
    if not logradouro:
        return ""
    normalized = str(logradouro).strip().upper()
    normalized = re.sub(r"^AV\.?\s+", "AVENIDA ", normalized)
    normalized = re.sub(r"^R\.?\s+", "RUA ", normalized)
    normalized = re.sub(r"^ROD\.?\s+", "RODOVIA ", normalized)
    normalized = re.sub(r"^AL\.?\s+", "ALAMEDA ", normalized)
    normalized = re.sub(r"^EST\.?\s+", "ESTRADA ", normalized)
    normalized = re.sub(r"^TRAV\.?\s+", "TRAVESSA ", normalized)
    normalized = re.sub(r"^PCA\.?\s+", "PRACA ", normalized)
    normalized = re.sub(r"^PC\.?\s+", "PRACA ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _cache_keys_geocoding(logradouro: str, numero: str, cidade: str, uf: str):
    """
    Gera (endereco_canonico, endereco_normalizado) compatível com o
    geocoding_engine — que indexa enderecos_cache por endereco_normalizado.
    """
    log = _normalizar_prefixo_logradouro(logradouro)
    numero = str(numero or "").strip()
    cidade = str(cidade or "").strip().upper()
    uf = str(uf or "").strip().upper()

    endereco_canonico = re.sub(
        r"\s+", " ", f"{log} {numero}, {cidade} - {uf}".replace(" ,", ",")
    ).strip()
    endereco_normalizado = normalize_for_cache(endereco_canonico)
    return endereco_canonico, endereco_normalizado


def _carregamento_descricao(tenant_id: int, input_id: str) -> str | None:
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            SELECT descricao
            FROM historico_pdv_jobs
            WHERE tenant_id = %s AND input_id = %s AND descricao IS NOT NULL
            ORDER BY criado_em DESC
            LIMIT 1;
            """,
            (tenant_id, input_id),
        )
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


# ==========================================================
# 📥 Download XLSX de PDVs válidos (gerado sob demanda do banco)
# GET /pdv/processamentos/{input_id}/download-validos
# ==========================================================

@router.get(
    "/processamentos/{input_id}/download-validos",
    dependencies=[Depends(verify_token)],
    tags=["Jobs"],
)
def download_validos(request: Request, input_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]

    input_id = _normalizar_input_id(input_id)

    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """
            SELECT
                cnpj, logradouro, numero, bairro, cidade, uf, cep,
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
        raise HTTPException(
            status_code=404,
            detail="Nenhum PDV válido encontrado para este carregamento.",
        )

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})

    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
        df.to_excel(writer, index=False, sheet_name="validos")
    buffer.seek(0)

    filename = f"pdvs_validos_{input_id}.xlsx"
    return StreamingResponse(
        buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


# ==========================================================
# 📥 Download XLSX de inválidos por carregamento (input_id)
# GET /pdv/processamentos/{input_id}/download-invalidos
# ==========================================================

@router.get(
    "/processamentos/{input_id}/download-invalidos",
    dependencies=[Depends(verify_token)],
    tags=["Jobs"],
)
def download_invalidos_por_input(request: Request, input_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]
    input_id = _normalizar_input_id(input_id)
    return _gerar_xlsx_invalidos_stream(tenant_id, input_id)


# ==========================================================
# 📍 Listar PDVs de um carregamento (paginado, com filtros)
# GET /pdv/processamentos/{input_id}/pdvs
# ==========================================================

@router.get(
    "/processamentos/{input_id}/pdvs",
    dependencies=[Depends(verify_token)],
    tags=["Locais"],
)
def listar_pdvs_do_carregamento(
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
    user = request.state.user
    tenant_id = user["tenant_id"]

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

    conn = get_connection()
    try:
        total_df = pd.read_sql_query(
            f"SELECT COUNT(*) AS total FROM pdvs WHERE {where_clause};",
            conn,
            params=tuple(params),
        )
        total = int(total_df.iloc[0]["total"]) if not total_df.empty else 0

        df = pd.read_sql_query(
            f"""
            SELECT
                id, tenant_id, input_id, descricao,
                cnpj, logradouro, numero, bairro, cidade, uf, cep,
                pdv_lat, pdv_lon, pdv_endereco_completo,
                status_geolocalizacao, pdv_vendas,
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
            SELECT COUNT(*) AS manuais
            FROM pdvs
            WHERE tenant_id = %s
              AND input_id = %s
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


# ==========================================================
# ➕ Inserção manual de PDVs já geocodificados (upload XLSX)
# POST /pdv/processamentos/{input_id}/pdvs-manuais
#
# REGRAS:
# - Planilha já vem com pdv_lat / pdv_lon — NÃO geocodifica
# - Unicidade por CNPJ dentro do carregamento (input_id)
# - Atualiza enderecos_cache pela chave canônica
# - Registra linha em historico_pdv_jobs (status=manual_insert)
# ==========================================================

@router.post(
    "/processamentos/{input_id}/pdvs-manuais",
    dependencies=[Depends(verify_token)],
    tags=["Locais"],
)
def inserir_pdvs_manuais(
    request: Request,
    input_id: str,
    file: UploadFile = File(...),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    if user.get("role") not in [
        "sales_router_adm",
        "tenant_adm",
        "tenant_operacional",
    ]:
        raise HTTPException(status_code=403, detail="Usuário sem permissão.")

    input_id = _normalizar_input_id(input_id)

    # ------------------------------------------------------
    # Carregamento precisa existir para este tenant
    # ------------------------------------------------------
    descricao = _carregamento_descricao(tenant_id, input_id)
    if descricao is None:
        raise HTTPException(
            status_code=404,
            detail="Carregamento não encontrado para este tenant.",
        )

    # ------------------------------------------------------
    # Leitura do arquivo
    # ------------------------------------------------------
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in [".xlsx", ".xls"]:
        raise HTTPException(
            status_code=400,
            detail="Formato inválido. Envie um arquivo XLSX.",
        )

    try:
        conteudo = file.file.read()
        df = pd.read_excel(io.BytesIO(conteudo), dtype=str, engine="openpyxl").fillna("")
    except Exception as e:
        logger.error(f"❌ Erro ao ler planilha manual: {e}", exc_info=True)
        raise HTTPException(status_code=400, detail=f"Falha ao ler a planilha: {e}")

    if df.empty:
        raise HTTPException(status_code=400, detail="Planilha vazia.")

    df.columns = [_normalizar_nome_coluna(c) for c in df.columns]

    colunas_obrigatorias = [
        "cnpj", "logradouro", "numero", "bairro",
        "cidade", "uf", "cep", "pdv_lat", "pdv_lon",
    ]
    faltantes = [c for c in colunas_obrigatorias if c not in df.columns]
    if faltantes:
        raise HTTPException(
            status_code=400,
            detail=f"Colunas ausentes na planilha: {', '.join(faltantes)}",
        )

    tem_vendas = "pdv_vendas" in df.columns
    # Campos opcionais (CVRPTW + identificação). Se presentes na
    # planilha, são parseados e persistidos no PDV; ausência não é erro.
    tem_jan_ini = "janela_atendimento_inicio" in df.columns
    tem_jan_fim = "janela_atendimento_fim" in df.columns
    tem_t_atend = "tempo_atendimento_min" in df.columns
    tem_estrat = "is_estrategico" in df.columns
    tem_razao = "razao_social" in df.columns
    tem_fantasia = "nome_fantasia" in df.columns

    # ------------------------------------------------------
    # Helpers de parsing
    # ------------------------------------------------------
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
        """Aceita HH:MM, HH:MM:SS, int/str de minutos. Retorna int ou None."""
        if valor is None:
            return None
        v = str(valor).strip()
        if not v or v.lower() in {"nan", "none", "null"}:
            return None
        m = re.match(r"^(\d{1,2}):(\d{2})(?::\d{2}(?:\.\d+)?)?$", v)
        if m:
            h, mm = int(m.group(1)), int(m.group(2))
            if 0 <= h < 24 and 0 <= mm < 60:
                return h * 60 + mm
            return None
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

    # ------------------------------------------------------
    # CNPJs já existentes no carregamento
    # ------------------------------------------------------
    reader = DatabaseReader()
    cnpjs_existentes = set(reader.buscar_cnpjs_existentes(tenant_id, input_id))

    pdvs_para_inserir: list[PDV] = []
    cache_updates: list[tuple[str, str, float, float]] = []
    cnpjs_no_lote: set[str] = set()

    invalidos: list[dict] = []
    duplicados: list[str] = []

    total_linhas = len(df)

    for posicao, (_, row) in enumerate(df.iterrows(), start=2):  # linha 1 = cabeçalho
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

        # Validações
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

        # Chaves de cache compatíveis com o geocoding_engine
        endereco_canonico, endereco_normalizado = _cache_keys_geocoding(
            logradouro, numero, cidade, uf
        )

        # O cache é indexado por endereço — vale para qualquer linha válida,
        # inclusive CNPJ duplicado (mesmo endereço continua útil no cache).
        cache_updates.append((endereco_canonico, endereco_normalizado, lat, lon))

        # Unicidade por CNPJ dentro do carregamento
        if cnpj in cnpjs_existentes or cnpj in cnpjs_no_lote:
            duplicados.append(cnpj)
            continue

        cnpjs_no_lote.add(cnpj)

        cep_fmt = f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else ""
        base_endereco = (
            f"{logradouro} {numero}, {bairro}, {cidade} - {uf}"
            if numero else
            f"{logradouro}, {bairro}, {cidade} - {uf}"
        )
        if cep_fmt:
            base_endereco = f"{base_endereco}, {cep_fmt}"
        endereco_completo = f"{base_endereco}, Brasil"

        # Opcionais (CVRPTW + identificação)
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

    # ------------------------------------------------------
    # Persistência
    # ------------------------------------------------------
    writer = DatabaseWriter()

    inseridos = 0
    if pdvs_para_inserir:
        inseridos = writer.inserir_pdvs(pdvs_para_inserir) or 0

    # Atualiza o cache (endereco_normalizado) para todas as linhas válidas
    for endereco_canonico, endereco_normalizado, lat, lon in cache_updates:
        writer.salvar_cache_geocoding(
            endereco_canonico,
            endereco_normalizado,
            lat,
            lon,
            origem="manual_insert",
        )

    # Inserção manual NÃO cria linha própria no histórico — apenas atualiza o
    # input de destino. O card do histórico é recalculado ao vivo da tabela pdvs.
    logger.info(
        f"➕ Inserção manual | tenant={tenant_id} input_id={input_id} "
        f"inseridos={inseridos} duplicados={len(duplicados)} invalidos={len(invalidos)}"
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

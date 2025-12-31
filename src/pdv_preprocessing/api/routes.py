#sales_router/src/pdv_preprocessing/api/routes.py

# ==========================================================
# 📦 src/pdv_preprocessing/api/routes.py
# ==========================================================
from fastapi import APIRouter, HTTPException, Query, Depends, Request
from loguru import logger
from database.db_connection import get_connection
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.entities.pdv_entity import PDV
from pdv_preprocessing.domain.address_normalizer import (
    normalize_base,
    normalize_for_cache,
)
from pdv_preprocessing.domain.utils_geo import coordenada_generica
from .dependencies import verify_token

from datetime import datetime, timedelta

import pandas as pd
import numpy as np
from pydantic import BaseModel

from uuid import UUID
import re

router = APIRouter()

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
    cnpj: str,
    logradouro: str = None,
    numero: str = None,
    bairro: str = None,
    cidade: str = None,
    uf: str = None,
    cep: str = None,
    lat: float = None,
    lon: float = None,
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    if user.get("role") not in ["sales_router_adm", "tenant_adm", "tenant_operacional"]:
        raise HTTPException(status_code=403, detail="Usuário sem permissão.")

    conn = get_connection()
    reader = DatabaseReader(conn)
    writer = DatabaseWriter()

    existente = reader.buscar_pdv_por_cnpj(tenant_id, cnpj)
    if not existente:
        conn.close()
        raise HTTPException(status_code=404, detail="PDV não encontrado.")

    atualizado = {**existente}
    for campo, valor in {
        "logradouro": logradouro,
        "numero": numero,
        "bairro": bairro,
        "cidade": cidade,
        "uf": uf,
        "cep": cep,
        "pdv_lat": lat,
        "pdv_lon": lon,
    }.items():
        if valor is not None:
            atualizado[campo] = valor

    atualizado["pdv_endereco_completo"] = (
        f"{atualizado.get('logradouro','')}, {atualizado.get('numero','')}, "
        f"{atualizado.get('bairro','')}, {atualizado.get('cidade','')} - "
        f"{atualizado.get('uf','')}, {atualizado.get('cep','')}"
    )

    atualizado["status_geolocalizacao"] = "manual_edit"

    pdv = PDV(**{**atualizado, "tenant_id": tenant_id})
    writer.atualizar_pdv_completo(pdv)

    # 🔒 Atualiza cache SOMENTE via chave canônica
    if lat is not None and lon is not None and not coordenada_generica(lat, lon):
        writer.atualizar_cache_por_chave(
            cache_key=existente.get("endereco_cache_key"),
            nova_lat=lat,
            nova_lon=lon,
        )

    conn.close()

    return {
        "status": "success",
        "pdv": atualizado,
    }



# ==========================================================
# 🚀 Enfileirar novo processamento de PDVs (upload CSV)
# ==========================================================
from redis import Redis
from rq import Queue
from pdv_preprocessing.pdv_jobs import processar_csv

@router.post("/upload", dependencies=[Depends(verify_token)], tags=["Jobs"])
def upload_pdv(
    request: Request,
    arquivo: str = Query(..., description="Caminho do arquivo CSV dentro de /app/data"),
    descricao: str = Query(..., description="Descrição amigável do job"),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        conn_redis = Redis(host="redis", port=6379)
        queue = Queue("pdv_jobs", connection=conn_redis)

        job = queue.enqueue(
            processar_csv,
            tenant_id,
            arquivo,
            descricao,
            job_timeout=36000  # 🔥 10 horas de timeout
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
        job = queue.enqueue(processar_csv, tenant_id, arquivo, descricao)

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
# ==========================================================
from fastapi import UploadFile, File
import shutil
import os
from datetime import datetime

@router.post("/upload-file", dependencies=[Depends(verify_token)], tags=["Jobs"])
def upload_arquivo(
    request: Request,
    descricao: str = Query(..., description="Descrição amigável do job"),
    file: UploadFile = File(...),
):
    """
    Recebe um arquivo CSV enviado pelo cliente (multipart/form-data),
    salva no volume /app/data e enfileira o processamento automaticamente.
    """
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        # 📁 Cria diretório de destino
        base_dir = "/app/data"
        os.makedirs(base_dir, exist_ok=True)

        # 🕒 Gera nome único para o arquivo
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        nome_final = f"pdvs_{tenant_id}_{timestamp}_{file.filename}"
        caminho_final = os.path.join(base_dir, nome_final)

        # 💾 Salva o arquivo no container
        with open(caminho_final, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)

        # 🚀 Enfileira o processamento
        conn_redis = Redis(host="redis", port=6379)
        queue = Queue("pdv_jobs", connection=conn_redis)
        job = queue.enqueue(
            processar_csv,
            tenant_id,
            caminho_final,
            descricao,
            job_timeout=36000  # 🔥 10 horas de timeout
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
        }

    except Exception as e:
        logger.error(f"❌ Erro no upload multipart: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================================
# 🗺️ Gerar mapa de PDVs (autenticado)
# ==========================================================
from pathlib import Path
from pdv_preprocessing.visualization.pdv_plotting import buscar_pdvs, gerar_mapa_pdvs

@router.post("/gerar-mapa", dependencies=[Depends(verify_token)], tags=["Visualização"])
def gerar_mapa_pdv(
    request: Request,
    input_id: str = Query(..., description="UUID do input de PDVs"),
    uf: str = Query(None, description="UF opcional para filtrar"),
    cidade: str = Query(None, description="Cidade opcional para filtrar (prioritário)"),
):
    """
    Gera o mapa de PDVs para o tenant e input_id informados.
    Pode filtrar por UF ou Cidade.
    Retorna o caminho do arquivo HTML gerado.
    """
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        logger.info(f"🗺️ Solicitada geração de mapa | tenant={tenant_id} | input={input_id} | UF={uf or '--'} | Cidade={cidade or '--'}")

        dados = buscar_pdvs(tenant_id, input_id, uf, cidade)
        if not dados:
            raise HTTPException(status_code=404, detail="Nenhum PDV encontrado para os parâmetros informados.")

        output_dir = Path(f"/app/output/maps/{tenant_id}")
        output_dir.mkdir(parents=True, exist_ok=True)


        nome_arquivo = f"pdvs_{input_id}_{cidade or uf or 'BR'}.html".replace(" ", "_")
        output_path = output_dir / nome_arquivo

        gerar_mapa_pdvs(dados, output_path)

        if not output_path.exists():
            raise HTTPException(status_code=500, detail="Falha ao gerar o arquivo de mapa.")

        logger.success(f"✅ Mapa de PDVs disponível em {output_path}")

        return {
            "status": "success",
            "tenant_id": tenant_id,
            "input_id": input_id,
            "uf": uf,
            "cidade": cidade,
            "arquivo_html": str(output_path),
            "url_relativa": f"/output/maps/{tenant_id}/{nome_arquivo}"
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao gerar mapa de PDVs: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=str(e))


# ==========================================================
# 📥 Download do mapa de PDVs (autenticado)
# ==========================================================
from fastapi.responses import FileResponse
import os

@router.get("/download-mapa", dependencies=[Depends(verify_token)], tags=["Visualização"])
def download_mapa_pdv(
    request: Request,
    input_id: str = Query(..., description="UUID do input de PDVs"),
    uf: str = Query(None, description="UF opcional usada no nome do arquivo"),
    cidade: str = Query(None, description="Cidade opcional usada no nome do arquivo"),
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    try:
        nome_arquivo = f"pdvs_{input_id}_{cidade or uf or 'BR'}.html".replace(" ", "_")
        caminho_arquivo = f"/app/output/maps/{tenant_id}/{nome_arquivo}"

        if not os.path.exists(caminho_arquivo):
            raise HTTPException(status_code=404, detail=f"Arquivo não encontrado: {caminho_arquivo}")

        logger.info(
            f"📥 Download solicitado por {user.get('email', 'usuário desconhecido')} | tenant={tenant_id} | arquivo={caminho_arquivo}"
        )
        return FileResponse(
            caminho_arquivo,
            media_type="text/html",
            filename=os.path.basename(caminho_arquivo)
        )

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"❌ Erro ao baixar mapa: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail=f"Erro ao baixar mapa: {e}")


# ==========================================================
# 🌐 Visualizar mapa diretamente (redirect)
# ==========================================================
from fastapi.responses import RedirectResponse

@router.get("/ver-mapa", dependencies=[Depends(verify_token)], tags=["Visualização"])
def ver_mapa_pdv(
    request: Request,
    input_id: str = Query(..., description="UUID do input de PDVs"),
    uf: str = Query(None, description="UF opcional usada no nome do arquivo"),
    cidade: str = Query(None, description="Cidade opcional usada no nome do arquivo"),
):
    """
    Redireciona o navegador diretamente para o arquivo HTML do mapa gerado.
    A URL é resolvida dentro da rota do serviço (por exemplo via API Gateway).
    """
    user = request.state.user
    tenant_id = user["tenant_id"]

    nome_arquivo = f"pdvs_{input_id}_{cidade or uf or 'BR'}.html".replace(" ", "_")
    caminho_arquivo = f"/app/output/maps/{tenant_id}/{nome_arquivo}"

    if not os.path.exists(caminho_arquivo):
        raise HTTPException(status_code=404, detail=f"Mapa não encontrado: {caminho_arquivo}")

    # 🔗 Monta URL relativa usada pelo Gateway (ex: /output/maps/1/arquivo.html)
    url_relativa = f"/output/maps/{tenant_id}/{nome_arquivo}"

    logger.info(
        f"🌐 Visualização solicitada | tenant={tenant_id} | arquivo={url_relativa}"
    )

    return RedirectResponse(url=url_relativa, status_code=302)


# ==========================================================
# GET /pdv/ultimos  → LISTA OS ÚLTIMOS 10 JOBS
# ==========================================================
@router.get("/jobs/ultimos", dependencies=[Depends(verify_token)], tags=["Jobs"])
def listar_ultimos_jobs(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]

    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT *
        FROM (
            SELECT DISTINCT ON (input_id)
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
            FROM historico_pdv_jobs
            WHERE tenant_id = %s
            ORDER BY input_id, criado_em DESC
        ) t
        ORDER BY criado_em DESC
        LIMIT 20;
    """

    cur.execute(sql, (tenant_id,))
    rows = cur.fetchall()

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

    cur.close()
    conn.close()

    return {
        "total": len(jobs),
        "jobs": jobs,
    }



# ==========================================================
# 📋 Filtrar jobs — aceita DD/MM/YYYY e YYYY-MM-DD
# ==========================================================

from datetime import datetime

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

    sql = f"""
        SELECT *
        FROM (
            SELECT DISTINCT ON (input_id)
                tenant_id,
                job_id,
                input_id,
                descricao,
                status,
                total_processados,
                validos,
                invalidos,
                criado_em
            FROM historico_pdv_jobs
            WHERE {where_clause}
            ORDER BY input_id, criado_em DESC
        ) t
        ORDER BY criado_em DESC
        LIMIT 20;
    """

    conn = get_connection()
    df = pd.read_sql_query(sql, conn, params=tuple(params))
    conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})

    return {
        "total": int(len(df)),
        "jobs": df.to_dict(orient="records"),
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

    return {
        "total": int(len(df)),
        "jobs": df.to_dict(orient="records"),
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
        filtros.append("cidade ILIKE %s")
        params.append(f"%{cidade}%")

    if cnpj:
        filtros.append("cnpj = %s")
        params.append(cnpj)

    if logradouro:
        filtros.append("logradouro ILIKE %s")
        params.append(f"%{logradouro}%")

    if bairro:
        filtros.append("bairro ILIKE %s")
        params.append(f"%{bairro}%")

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



# ============================================================
# ✏️ Gestão de Locais — EDITAR (PUT /pdv/locais/{pdv_id})
# 👉 MVP: cache sempre por TEXTO NORMALIZADO
# ============================================================

from pydantic import BaseModel

class EditarLocalPayload(BaseModel):
    pdv_lat: float | None = None
    pdv_lon: float | None = None


@router.put("/locais/{pdv_id}", dependencies=[Depends(verify_token)], tags=["Locais"])
def editar_local(
    request: Request,
    pdv_id: int,
    payload: EditarLocalPayload,
):
    user = request.state.user
    tenant_id = user["tenant_id"]

    # --------------------------------------------------
    # 🔐 Permissão
    # --------------------------------------------------
    if user.get("role") not in ["sales_router_adm", "tenant_adm", "tenant_operacional"]:
        raise HTTPException(status_code=403, detail="Sem permissão para editar.")

    # --------------------------------------------------
    # 📥 Valida payload
    # --------------------------------------------------
    if payload.pdv_lat is None or payload.pdv_lon is None:
        raise HTTPException(status_code=400, detail="pdv_lat e pdv_lon são obrigatórios.")

    if coordenada_generica(payload.pdv_lat, payload.pdv_lon):
        raise HTTPException(status_code=400, detail="Coordenadas inválidas.")

    conn = get_connection()
    cur = conn.cursor()

    # --------------------------------------------------
    # 1️⃣ Busca CHAVE CANÔNICA EXISTENTE
    # --------------------------------------------------
    cur.execute(
        """
        SELECT endereco_cache_key
        FROM pdvs
        WHERE id = %s
          AND tenant_id = %s
        """,
        (pdv_id, tenant_id),
    )
    row = cur.fetchone()

    if not row or not row[0]:
        conn.close()
        raise HTTPException(
            status_code=500,
            detail="PDV sem endereco_cache_key. Reprocessar input."
        )

    cache_key = row[0]

    # --------------------------------------------------
    # 2️⃣ Atualiza PDV
    # --------------------------------------------------
    cur.execute(
        """
        UPDATE pdvs
        SET
            pdv_lat = %s,
            pdv_lon = %s,
            status_geolocalizacao = 'manual_edit',
            atualizado_em = NOW()
        WHERE id = %s
          AND tenant_id = %s
        RETURNING
            id, tenant_id, input_id, descricao, cnpj,
            logradouro, numero, bairro, cidade, uf, cep,
            pdv_endereco_completo, pdv_lat, pdv_lon,
            status_geolocalizacao, pdv_vendas,
            criado_em, atualizado_em;
        """,
        (
            payload.pdv_lat,
            payload.pdv_lon,
            pdv_id,
            tenant_id,
        ),
    )

    atualizado = cur.fetchone()
    conn.commit()
    conn.close()

    colunas = [
        "id", "tenant_id", "input_id", "descricao", "cnpj",
        "logradouro", "numero", "bairro", "cidade", "uf", "cep",
        "pdv_endereco_completo", "pdv_lat", "pdv_lon",
        "status_geolocalizacao", "pdv_vendas",
        "criado_em", "atualizado_em",
    ]
    pdv_dict = dict(zip(colunas, atualizado))

    # --------------------------------------------------
    # 3️⃣ Atualiza CACHE — ÚNICO MÉTODO PERMITIDO
    # --------------------------------------------------
    writer = DatabaseWriter()
    writer.atualizar_cache_por_chave(
        cache_key=cache_key,
        nova_lat=payload.pdv_lat,
        nova_lon=payload.pdv_lon,
    )

    return {
        "status": "success",
        "pdv": pdv_dict,
    }


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
        from redis import Redis
        from rq.job import Job

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
from redis import Redis
from rq.job import Job

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

        # -------------------------
        # STATUS PADRONIZADO
        # -------------------------
        if job.is_finished:
            return {
                "job_id": job.id,
                "status": "done",
                "progress": 100,
                "step": "Finalizado"
            }

        if job.is_failed:
            return {
                "job_id": job.id,
                "status": "error",
                "progress": progresso,
                "step": etapa
            }

        # Qualquer outro estado → sempre RUNNING
        return {
            "job_id": job.id,
            "status": "running",
            "progress": progresso,
            "step": etapa
        }

    except Exception as e:
        logger.error(f"❌ Erro ao consultar progresso do job {job_id}: {e}", exc_info=True)
        raise HTTPException(status_code=404, detail=f"Job não encontrado ou expirado: {e}")


# ==========================================================
# 📥 Download CSV de registros inválidos
# ==========================================================
from fastapi.responses import FileResponse
import os

@router.get(
    "/jobs/{job_id}/download-invalidos",
    dependencies=[Depends(verify_token)],
    tags=["Jobs"],
)
def download_invalidos(request: Request, job_id: str):
    user = request.state.user
    tenant_id = user["tenant_id"]

    conn = get_connection()
    cur = conn.cursor()

    cur.execute(
        """
        SELECT arquivo_invalidos
        FROM historico_pdv_jobs
        WHERE tenant_id = %s
          AND job_id = %s
          AND arquivo_invalidos IS NOT NULL
        LIMIT 1;
        """,
        (tenant_id, job_id),
    )

    row = cur.fetchone()
    conn.close()

    if not row:
        raise HTTPException(
            status_code=404,
            detail="Arquivo de inválidos não encontrado para este job.",
        )

    caminho_arquivo = row[0]

    if not os.path.exists(caminho_arquivo):
        raise HTTPException(
            status_code=404,
            detail=f"Arquivo não existe no disco: {caminho_arquivo}",
        )

    return FileResponse(
        path=caminho_arquivo,
        media_type="text/csv",
        filename=os.path.basename(caminho_arquivo),
    )

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
from .dependencies import verify_token
import pandas as pd
import numpy as np

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

    # 🔒 Permissão
    if user.get("role") not in ["sales_router_adm", "tenant_adm", "tenant_operacional"]:
        raise HTTPException(status_code=403, detail="Usuário sem permissão para editar PDVs.")

    conn = get_connection()
    reader = DatabaseReader(conn)
    writer = DatabaseWriter(conn)

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
        f"{atualizado.get('logradouro', '')}, {atualizado.get('numero', '')}, "
        f"{atualizado.get('bairro', '')}, {atualizado.get('cidade', '')} - "
        f"{atualizado.get('uf', '')}, {atualizado.get('cep', '')}"
    )
    atualizado["status_geolocalizacao"] = "manual_edit"

    pdv = PDV(**{**atualizado, "tenant_id": tenant_id})
    writer.inserir_pdvs([pdv])
    conn.close()

    # 🧹 Sanitiza antes de devolver
    for k, v in atualizado.items():
        if isinstance(v, float) and (np.isnan(v) or np.isinf(v)):
            atualizado[k] = None

    logger.info(f"📝 PDV {cnpj} atualizado por {user['email']} (tenant={tenant_id})")

    return {
        "status": "success",
        "message": "PDV atualizado com sucesso",
        "usuario": user,
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

        job = queue.enqueue(processar_csv, tenant_id, arquivo, descricao)

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
        job = queue.enqueue(processar_csv, tenant_id, caminho_final, descricao)

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
# GET /pdv/jobs/ultimos  → LISTA OS ÚLTIMOS 10 JOBS
# ==========================================================
@router.get("/jobs/ultimos", dependencies=[Depends(verify_token)], tags=["Jobs"])
def listar_ultimos_jobs(request: Request):
    user = request.state.user
    tenant_id = user["tenant_id"]

    conn = get_connection()
    cur = conn.cursor()

    sql = """
        SELECT 
            id, tenant_id, job_id, arquivo, status, total_processados,
            validos, invalidos, arquivo_invalidos, mensagem, criado_em,
            inseridos, sobrescritos, descricao, input_id
        FROM historico_pdv_jobs
        WHERE tenant_id = %s
        ORDER BY criado_em DESC
        LIMIT 10;
    """

    cur.execute(sql, (tenant_id,))
    rows = cur.fetchall()

    colunas = [
        "id", "tenant_id", "job_id", "arquivo", "status",
        "total_processados", "validos", "invalidos", "arquivo_invalidos",
        "mensagem", "criado_em", "inseridos", "sobrescritos",
        "descricao", "input_id"
    ]

    jobs = [dict(zip(colunas, row)) for row in rows]

    cur.close()
    conn.close()

    return {"total": len(jobs), "jobs": jobs}


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

    # Converte datas corretamente
    data_inicio_dt = parse_data(data_inicio)
    data_fim_dt = parse_data(data_fim)

    # Filtros SQL
    filtros = ["tenant_id = %s"]
    params = [tenant_id]

    if data_inicio_dt:
        filtros.append("DATE(criado_em) >= %s")
        params.append(str(data_inicio_dt))  # yyyy-mm-dd

    if data_fim_dt:
        filtros.append("DATE(criado_em) <= %s")
        params.append(str(data_fim_dt))

    if descricao:
        filtros.append("descricao ILIKE %s")
        params.append(f"%{descricao}%")

    where_clause = " AND ".join(filtros)

    sql = f"""
        SELECT
            job_id,
            descricao,
            status,
            total_processados,
            validos,
            invalidos,
            criado_em
        FROM historico_pdv_jobs
        WHERE {where_clause}
        ORDER BY criado_em DESC
        LIMIT 10;
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
        SELECT id, tenant_id, input_id, descricao, arquivo, status,
               total_processados, validos, invalidos, arquivo_invalidos,
               mensagem, criado_em
        FROM historico_pdv_jobs
        WHERE tenant_id = %s
        ORDER BY criado_em DESC
        LIMIT 100;
        """,
        conn,
        params=(tenant_id,),
    )
    conn.close()

    df = df.astype(object).replace({np.nan: None, np.inf: None, -np.inf: None})
    logger.info(f"📄 {len(df)} jobs listados para tenant {tenant_id}")
    return {"total": int(len(df)), "jobs": df.to_dict(orient="records")}


# ==========================================================
# 🔍 Buscar detalhes de um job específico
# ==========================================================
@router.get("/jobs/{job_id}", dependencies=[Depends(verify_token)], tags=["Jobs"])
def detalhar_job(request: Request, job_id: str):
    from redis import Redis
    from rq.job import Job

    try:
        conn_redis = Redis(host="redis", port=6379)
        job = Job.fetch(job_id, connection=conn_redis)

        return {
            "job_id": job.id,
            "status": job.get_status(),
            "meta": job.meta,
            "tenant_id": job.args[0] if job.args else None,
            "arquivo": job.args[1] if len(job.args) > 1 else None,
            "descricao": job.args[2] if len(job.args) > 2 else None,
        }
    except Exception as e:
        raise HTTPException(status_code=404, detail=f"Job não encontrado: {e}")


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

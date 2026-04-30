#sales_router/src/routing_engine/api/routes.py

from datetime import date
from typing import Any, Optional

import pandas as pd
from fastapi import APIRouter, Depends, UploadFile, File, HTTPException, Form, Request
from fastapi.responses import FileResponse, JSONResponse
from routing_engine.api.dependencies import verify_token
from pydantic import BaseModel

from routing_engine.infrastructure.queue_factory import get_queue
from routing_engine.workers.routing_jobs import processar_routing
from routing_engine.application.prepared_groups_routing_use_case import executar_prepared_groups_master_job
from routing_engine.visualization.map_use_case import GenerateMapUseCase
from routing_engine.application.holiday_utils import dias_uteis as calcular_dias_uteis
from routing_engine.infrastructure.agenda_repository import (
    AgendaRotaRow, AgendaVisitaRow,
    criar_agenda, buscar_agenda, listar_agendas,
    atualizar_data_rota, datas_ocupadas_consultor,
    listar_visitas_flat, atualizar_status_visita, atualizar_status_visitas_lote,
)

from rq.job import Job
from redis import Redis
import os
import uuid
import json

router = APIRouter()

MAX_UPLOAD_MB = 50


class PreparedGroupPDV(BaseModel):
    pdv_id: int
    lat: float
    lon: float
    cidade: str | None = None
    uf: str | None = None
    freq_visita: float | None = None
    cnpj: str | None = None
    nome_fantasia: str | None = None
    logradouro: str | None = None
    numero: str | None = None
    bairro: str | None = None
    cep: str | None = None
    grupo_utilizado: str | None = None
    fonte_grupo: str | None = None


class PreparedGroup(BaseModel):
    group_id: str
    group_type: str = "cluster"
    cluster_id: int | None = None
    run_id: int | None = None
    centro_lat: float
    centro_lon: float
    pdvs: list[PreparedGroupPDV]


class PreparedGroupsParams(BaseModel):
    modo_calculo: str = "frequencia"
    dias_uteis: int
    frequencia_visita: float
    min_pdvs_rota: int
    max_pdvs_rota: int
    service_min: float = 30.0
    v_kmh: float = 60.0
    alpha_path: float = 1.3
    twoopt: bool = False
    preserve_sequence: bool = False


class PreparedGroupsRequest(BaseModel):
    tenant_id: int
    routing_id: str
    parent_job_id: str | None = None
    requested_by: str | None = None
    mode: str = "balanceado"
    params: PreparedGroupsParams
    groups: list[PreparedGroup]


class CriarAgendaRequest(BaseModel):
    tenant_id: int
    nome: str
    data_inicio: date
    data_fim: date


class AtualizarDataRotaRequest(BaseModel):
    nova_data: date


_STATUS_VALIDOS = {"a_realizar", "realizada", "cancelada"}


class AtualizarStatusVisitaRequest(BaseModel):
    status: Optional[str] = None          # 'a_realizar' | 'realizada' | 'cancelada'
    data_realizacao: Optional[date] = None
    data_prevista: Optional[date] = None  # reschedule override for planned date
    limpar_data_prevista: bool = False    # set True to clear the override


class AtualizarStatusVisitasLoteRequest(BaseModel):
    visita_ids: list[str]
    status: Optional[str] = None
    data_realizacao: Optional[date] = None
    data_prevista: Optional[date] = None
    limpar_data_prevista: bool = False


# ============================================================
# REDIS HELPER
# ============================================================

def get_redis_conn():
    redis_url = os.getenv("REDIS_URL", "redis://redis:6379/0")
    return Redis.from_url(redis_url)


# ============================================================
# HEALTH
# ============================================================

@router.get("/health")
def health():
    return {"status": "ok", "service": "routing_engine"}


@router.post(
    "/internal/prepared-groups/balanced-routing",
    dependencies=[Depends(verify_token)]
)
def prepared_groups_balanced_routing(
    body: PreparedGroupsRequest,
    request: Request,
):
    user = request.state.user
    user_tenant_id = user.get("tenant_id")

    if user_tenant_id is not None and int(user_tenant_id) != body.tenant_id:
        raise HTTPException(status_code=403, detail="tenant_id divergente do token")

    if body.mode != "balanceado":
        raise HTTPException(status_code=400, detail="Este endpoint aceita apenas modo balanceado")

    if not body.groups:
        raise HTTPException(status_code=400, detail="Nenhum grupo informado")

    queue = get_queue("routing_prepared_groups")
    job = queue.enqueue(
        executar_prepared_groups_master_job,
        body.dict(),
        job_timeout=36000,
    )

    job.meta.update({
        "progress": 0,
        "step": "Job enfileirado",
        "routing_id": body.routing_id,
        "summary": {
            "groups_received": len(body.groups),
        },
    })
    job.save_meta()

    return {
        "status": "queued",
        "job_id": job.id,
        "routing_id": body.routing_id,
    }


# ============================================================
# UPLOAD
# ============================================================

@router.post(
    "/upload",
    dependencies=[Depends(verify_token)]
)
async def routing_upload(
    file: UploadFile = File(...),
    params: str | None = Form(None)
):

        if not file.filename.endswith(".xlsx"):
            raise HTTPException(status_code=400, detail="Arquivo deve ser XLSX")

        file_bytes = await file.read()

        if len(file_bytes) > MAX_UPLOAD_MB * 1024 * 1024:
            raise HTTPException(
                status_code=400,
                detail=f"Arquivo muito grande (máx {MAX_UPLOAD_MB}MB)"
            )

        try:
            parsed_params = json.loads(params) if params else {}
        except Exception:
            raise HTTPException(400, "Parâmetros inválidos")

        UPLOAD_DIR = "/app/data/uploads"
        os.makedirs(UPLOAD_DIR, exist_ok=True)

        file_id = str(uuid.uuid4())
        file_path = f"{UPLOAD_DIR}/{file_id}.xlsx"

        with open(file_path, "wb") as f:
            f.write(file_bytes)

        queue = get_queue("routing_batch")

        job = queue.enqueue(
            processar_routing,
            file_path,
            parsed_params,
            job_timeout=36000
        )

        return {
            "status": "queued",
            "job_id": job.id
        }

# ============================================================
# JOB STATUS
# ============================================================

@router.get(
    "/job/{job_id}",
    dependencies=[Depends(verify_token)]
)
def job_status(job_id: str):

    conn = get_redis_conn()

    try:
        job = Job.fetch(job_id, connection=conn)
    except Exception:
        return {
            "status": "not_found"
        }

    if not job:
        return {
            "status": "not_found"
        }

    response = {
        "job_id": job.id,
        "status": job.get_status(),
        "progress": job.meta.get("progress"),
        "step": job.meta.get("step"),
        "summary": job.meta.get("summary")
    }

    # ✔ sucesso
    if job.is_finished:
        response["result"] = job.result

    # ✔ erro (SIMPLES E CORRETO)
    if job.is_failed:
        response["error"] = job.meta.get("error") or {
            "mensagem": "Erro desconhecido no processamento"
        }

    return response

# ============================================================
# DOWNLOAD
# ============================================================

@router.get(
    "/job/{job_id}/download",
    dependencies=[Depends(verify_token)]
)
def download_result(job_id: str):

    conn = get_redis_conn()

    try:
        job = Job.fetch(job_id, connection=conn)
    except:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.get_status() != "finished":
        raise HTTPException(
            status_code=400,
            detail="Job ainda não finalizado"
        )

    output_file = job.meta.get("output_file")

    if not output_file or not os.path.exists(output_file):
        raise HTTPException(
            status_code=404,
            detail="Arquivo não encontrado"
        )

    return FileResponse(
        output_file,
        filename=f"routing_{job_id}.xlsx",
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


# ============================================================
# MAP (PADRÃO CORRETO)
# ============================================================

@router.get(
    "/job/{job_id}/map",
    dependencies=[Depends(verify_token)]
)
def job_map(job_id: str):

    conn = get_redis_conn()

    try:
        job = Job.fetch(job_id, connection=conn)
    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.get_status() != "finished":
        raise HTTPException(
            status_code=400,
            detail="Job ainda não finalizado"
        )

    json_path = job.meta.get("output_json")

    if not json_path or not os.path.exists(json_path):
        raise HTTPException(
            status_code=404,
            detail="JSON não encontrado"
        )

    uc = GenerateMapUseCase()
    geojson = uc.execute(json_path)

    return JSONResponse(content=geojson)


# ============================================================
# AGENDA
# ============================================================

@router.get(
    "/agenda",
    dependencies=[Depends(verify_token)],
)
def listar_agendas_route(request: Request):
    user = request.state.user
    tenant_id = int(user.get("tenant_id", 0))
    return listar_agendas(tenant_id)


@router.post(
    "/job/{job_id}/agenda",
    dependencies=[Depends(verify_token)],
    status_code=201,
)
def criar_agenda_route(job_id: str, body: CriarAgendaRequest, request: Request):
    redis_conn = get_redis_conn()
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        raise HTTPException(status_code=404, detail="Job não encontrado")

    if job.get_status() != "finished":
        raise HTTPException(status_code=409, detail="Job ainda não finalizado")

    output_file = job.meta.get("output_file")
    if not output_file or not os.path.exists(output_file):
        raise HTTPException(status_code=404, detail="Arquivo de resultado não encontrado")

    # Parse the routing output
    try:
        df_detalhe = pd.read_excel(output_file, sheet_name="roteirizacao_detalhe")
        df_resumo = pd.read_excel(output_file, sheet_name="roteirizacao_resumo")
    except Exception as e:
        raise HTTPException(status_code=422, detail=f"Erro ao ler resultado: {e}")

    # Compute working days in the requested period
    dias = calcular_dias_uteis(body.data_inicio, body.data_fim)
    if not dias:
        raise HTTPException(status_code=400, detail="Nenhum dia útil no período informado")

    # Build route index from summary: (consultor, rota_id) -> metrics
    resumo_idx: dict[tuple, dict] = {}
    for _, row in df_resumo.iterrows():
        key = (str(row.get("grupo_utilizado", "")), str(row.get("rota_id", "")))
        resumo_idx[key] = {
            "distancia_km": row.get("distancia_km"),
            "tempo_min": row.get("tempo_min"),
            "qtd_pdvs": row.get("qtd_pdvs"),
        }

    # Group PDVs by (consultor, rota_id), preserving sequencia order
    from collections import defaultdict
    grupos: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for _, row in df_detalhe.sort_values("sequencia").iterrows():
        consultor = str(row.get("grupo_utilizado", ""))
        rota_id = str(row.get("rota_id", ""))
        grupos[consultor][rota_id].append(row)

    # Assign calendar dates: for each consultant, sort their routes by rota_id
    # and map sequentially to available working days
    rotas_para_salvar: list[AgendaRotaRow] = []
    for consultor, rotas_dict in sorted(grupos.items()):
        rotas_ordenadas = sorted(rotas_dict.keys())  # R1, R10, R2... needs natural sort
        rotas_ordenadas.sort(key=lambda r: int(r[1:]) if r[1:].isdigit() else 0)

        for idx, rota_id in enumerate(rotas_ordenadas):
            if idx >= len(dias):
                raise HTTPException(
                    status_code=400,
                    detail=(
                        f"Período insuficiente: consultor '{consultor}' tem {len(rotas_ordenadas)} "
                        f"rotas mas o período tem apenas {len(dias)} dias úteis."
                    ),
                )
            data_rota = dias[idx]
            metricas = resumo_idx.get((consultor, rota_id), {})

            pdvs = [
                AgendaVisitaRow(
                    sequencia=int(r.get("sequencia", 0)),
                    cnpj=str(r["cnpj"]) if pd.notna(r.get("cnpj")) else None,
                    nome_fantasia=str(r["nome_fantasia"]) if pd.notna(r.get("nome_fantasia")) else None,
                    cidade=str(r["cidade"]) if pd.notna(r.get("cidade")) else None,
                    uf=str(r["uf"]) if pd.notna(r.get("uf")) else None,
                    lat=float(r["lat"]) if pd.notna(r.get("lat")) else None,
                    lon=float(r["lon"]) if pd.notna(r.get("lon")) else None,
                )
                for r in rotas_dict[rota_id]
            ]

            rotas_para_salvar.append(AgendaRotaRow(
                consultor=consultor,
                rota_id=rota_id,
                data=data_rota,
                distancia_km=metricas.get("distancia_km"),
                tempo_min=metricas.get("tempo_min"),
                qtd_pdvs=metricas.get("qtd_pdvs"),
                pdvs=pdvs,
            ))

    agenda_id = criar_agenda(
        tenant_id=body.tenant_id,
        job_id=job_id,
        nome=body.nome,
        data_inicio=body.data_inicio,
        data_fim=body.data_fim,
        dias_uteis=len(dias),
        rotas=rotas_para_salvar,
    )

    agenda = buscar_agenda(agenda_id, body.tenant_id)
    return agenda


@router.get(
    "/agenda/{agenda_id}",
    dependencies=[Depends(verify_token)],
)
def buscar_agenda_route(agenda_id: str, request: Request):
    user = request.state.user
    tenant_id = int(user.get("tenant_id", 0))
    agenda = buscar_agenda(agenda_id, tenant_id)
    if not agenda:
        raise HTTPException(status_code=404, detail="Agenda não encontrada")
    return agenda


@router.patch(
    "/agenda/rota/{rota_id}",
    dependencies=[Depends(verify_token)],
)
def atualizar_data_rota_route(rota_id: str, body: AtualizarDataRotaRequest, request: Request):
    user = request.state.user
    tenant_id = int(user.get("tenant_id", 0))

    # Validate new date is a working day
    dias = calcular_dias_uteis(body.nova_data, body.nova_data)
    if not dias:
        raise HTTPException(
            status_code=400,
            detail="A data informada é um fim de semana ou feriado nacional."
        )

    updated = atualizar_data_rota(rota_id, tenant_id, body.nova_data)
    if not updated:
        raise HTTPException(status_code=404, detail="Rota não encontrada")
    return updated


@router.get(
    "/agenda/{agenda_id}/export",
    dependencies=[Depends(verify_token)],
)
def exportar_agenda_route(agenda_id: str, request: Request):
    import io
    user = request.state.user
    tenant_id = int(user.get("tenant_id", 0))
    agenda = buscar_agenda(agenda_id, tenant_id)
    if not agenda:
        raise HTTPException(status_code=404, detail="Agenda não encontrada")

    rotas_rows = []
    visitas_rows = []
    for rota in agenda["rotas"]:
        rotas_rows.append({
            "Consultor": rota["consultor"],
            "Rota": rota["rota_id"],
            "Data": rota["data"],
            "PDVs": rota["qtd_pdvs"],
            "Distância (km)": rota["distancia_km"],
            "Tempo (min)": rota["tempo_min"],
            "Data alterada manualmente": "Sim" if rota["data_alterada_manualmente"] else "Não",
        })
        for v in rota["visitas"]:
            visitas_rows.append({
                "Consultor": rota["consultor"],
                "Rota": rota["rota_id"],
                "Data": rota["data"],
                "Sequência": v["sequencia"],
                "CNPJ": v["cnpj"],
                "Nome fantasia": v["nome_fantasia"],
                "Cidade": v["cidade"],
                "UF": v["uf"],
                "Latitude": v["lat"],
                "Longitude": v["lon"],
            })

    df_rotas = pd.DataFrame(rotas_rows)
    df_visitas = pd.DataFrame(visitas_rows)

    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        df_rotas.to_excel(writer, sheet_name="Rotas", index=False)
        df_visitas.to_excel(writer, sheet_name="Visitas", index=False)
    buf.seek(0)

    from fastapi.responses import StreamingResponse
    nome_arquivo = agenda["nome"].replace(" ", "_").lower()
    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f'attachment; filename="agenda_{nome_arquivo}.xlsx"'},
    )


@router.get(
    "/agenda/{agenda_id}/visitas",
    dependencies=[Depends(verify_token)],
)
def listar_visitas_route(agenda_id: str, request: Request):
    user = request.state.user
    tenant_id = int(user.get("tenant_id", 0))
    return listar_visitas_flat(agenda_id, tenant_id)


@router.patch(
    "/agenda/visita/{visita_id}/status",
    dependencies=[Depends(verify_token)],
)
def atualizar_status_visita_route(visita_id: str, body: AtualizarStatusVisitaRequest, request: Request):
    if body.status is not None and body.status not in _STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail=f"Status inválido. Use: {', '.join(_STATUS_VALIDOS)}.")
    if not any([body.status, body.data_realizacao, body.data_prevista, body.limpar_data_prevista]):
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar.")
    user = request.state.user
    tenant_id = int(user.get("tenant_id", 0))
    updated = atualizar_status_visita(
        visita_id, tenant_id,
        status=body.status,
        data_realizacao=body.data_realizacao,
        data_prevista=body.data_prevista,
        limpar_data_prevista=body.limpar_data_prevista,
    )
    if not updated:
        raise HTTPException(status_code=404, detail="Visita não encontrada")
    return updated


@router.patch(
    "/agenda/{agenda_id}/visitas/lote",
    dependencies=[Depends(verify_token)],
)
def atualizar_status_lote_route(agenda_id: str, body: AtualizarStatusVisitasLoteRequest, request: Request):
    if body.status is not None and body.status not in _STATUS_VALIDOS:
        raise HTTPException(status_code=400, detail=f"Status inválido. Use: {', '.join(_STATUS_VALIDOS)}.")
    if not body.visita_ids:
        raise HTTPException(status_code=400, detail="Nenhuma visita informada.")
    if not any([body.status, body.data_realizacao, body.data_prevista, body.limpar_data_prevista]):
        raise HTTPException(status_code=400, detail="Nenhum campo para atualizar.")
    user = request.state.user
    tenant_id = int(user.get("tenant_id", 0))
    updated = atualizar_status_visitas_lote(
        agenda_id, tenant_id, body.visita_ids,
        status=body.status,
        data_realizacao=body.data_realizacao,
        data_prevista=body.data_prevista,
        limpar_data_prevista=body.limpar_data_prevista,
    )
    return {"updated": updated}


@router.get(
    "/agenda/{agenda_id}/consultor/{consultor}/datas-ocupadas",
    dependencies=[Depends(verify_token)],
)
def datas_ocupadas_route(agenda_id: str, consultor: str, request: Request):
    user = request.state.user
    tenant_id = int(user.get("tenant_id", 0))
    return {"datas": datas_ocupadas_consultor(agenda_id, consultor, tenant_id)}


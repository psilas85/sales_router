import json
from pathlib import Path

from fastapi import APIRouter, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, StreamingResponse

from clusterization_engine.application import parser
from clusterization_engine.application.clusterization_service import (
    create_job,
    get_job,
    job_to_dict,
    normalize_params,
    _run_algorithm,
)

router = APIRouter(tags=["Clusterization"])


def _result_response(job_id: str, output_path: str):
    return FileResponse(
        output_path,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        filename=f"clusterization_{job_id}.xlsx",
    )


async def _create_upload_job(
    file: UploadFile,
    params_json: str | None,
    algoritmo: str | None = None,
    form_params: dict | None = None,
):
    try:
        parsed_params = json.loads(params_json) if params_json else {}
        if form_params:
            parsed_params.update({k: v for k, v in form_params.items() if v is not None})
        params = normalize_params(parsed_params, algoritmo=algoritmo)
        return await create_job(file, params)
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Parâmetros inválidos")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))


@router.post("/cluster/upload-planilha")
def upload_planilha(file: UploadFile = File(...)):
    return {"filename": file.filename, "status": "received"}


@router.post("/cluster/executar")
def executar_clusterizacao_placeholder(
    algoritmo: str = Form(..., description="Algoritmo de clusterizacao (ex: kmeans, sweep, custom)"),
    file: UploadFile = File(...)
):
    return {"algoritmo": algoritmo, "filename": file.filename, "status": "executando"}


@router.post("/api/v1/upload", status_code=202)
async def upload_clusterization(
    file: UploadFile = File(...),
    params: str | None = Form(None),
):
    job = await _create_upload_job(file=file, params_json=params)
    return {"status": "queued", "job_id": job.job_id}


@router.post("/cluster/jobs", status_code=202)
async def iniciar_clusterizacao_legacy(
    algoritmo: str = Form("kmeans", description="Algoritmo: kmeans, capacitated_sweep, sweep ou dense_subset"),
    file: UploadFile = File(...),
    n_clusters: int | None = Form(None),
    max_pdv_cluster: int | None = Form(None),
    sheet_name: str | None = Form(None),
    consultor_prefix: str = Form("Consultor"),
    latitude_col: str | None = Form(None),
    longitude_col: str | None = Form(None),
    random_state: int = Form(42),
):
    job = await _create_upload_job(
        file=file,
        params_json=None,
        algoritmo=algoritmo,
        form_params={
            "n_clusters": n_clusters,
            "max_pdv_cluster": max_pdv_cluster,
            "sheet_name": sheet_name or None,
            "consultor_prefix": consultor_prefix or "Consultor",
            "latitude_col": latitude_col or None,
            "longitude_col": longitude_col or None,
            "random_state": random_state,
        },
    )
    return {
        "job_id": job.job_id,
        "status": job.status,
        "status_url": f"/cluster/jobs/{job.job_id}",
        "result_url": f"/cluster/jobs/{job.job_id}/resultado",
    }


@router.get("/api/v1/job/{job_id}")
def consultar_job(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nao encontrado.")
    data = job_to_dict(job)
    return {
        "job_id": data["job_id"],
        "status": data["status"],
        "progress": data["progress"],
        "step": data["message"],
        "summary": data.get("params"),
        "error": data.get("error"),
        "result": {"output_file": data.get("output_path")} if data["status"] == "finished" else None,
    }


@router.get("/cluster/jobs/{job_id}")
def consultar_job_legacy(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nao encontrado.")
    return job_to_dict(job)


@router.get("/api/v1/job/{job_id}/download")
def baixar_resultado(job_id: str):
    job = get_job(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job nao encontrado.")
    if job.status != "finished":
        raise HTTPException(status_code=409, detail="Resultado ainda nao esta disponivel.")
    if not job.output_path or not Path(job.output_path).exists():
        raise HTTPException(status_code=404, detail="Arquivo de resultado nao encontrado.")

    return _result_response(job_id, job.output_path)


@router.get("/cluster/jobs/{job_id}/resultado")
def baixar_resultado_legacy(job_id: str):
    return baixar_resultado(job_id)


@router.post("/cluster/executar-clusterizacao")
def executar_clusterizacao(
    algoritmo: str = Form(..., description="Algoritmo de clusterizacao (ex: kmeans, sweep, custom)"),
    file: UploadFile = File(...),
    n_clusters: int = Form(3),
    max_pdv_cluster: int | None = Form(None),
    sheet_name: str | None = Form(None),
    consultor_prefix: str = Form("Consultor"),
    latitude_col: str | None = Form(None),
    longitude_col: str | None = Form(None),
    random_state: int = Form(42),
):
    params = normalize_params(
        {
            "n_clusters": n_clusters,
            "max_pdv_cluster": max_pdv_cluster,
            "sheet_name": sheet_name,
            "consultor_prefix": consultor_prefix,
            "latitude_col": latitude_col,
            "longitude_col": longitude_col,
            "random_state": random_state,
        },
        algoritmo=algoritmo,
    )
    sheet: str | int = 0 if not params.sheet_name else params.sheet_name
    df = parser.parse_entregas_planilha(file.file, sheet_name=sheet)
    df = _run_algorithm(df, params)

    output_bytes = parser.gerar_output_planilha(df)
    return StreamingResponse(
        iter([output_bytes]),
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": "attachment; filename=resultado_clusterizacao.xlsx"},
    )

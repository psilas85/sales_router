from dataclasses import asdict, dataclass
from datetime import datetime
import json
import math
import os
from pathlib import Path
import time
from typing import Any
import shutil
import uuid

from fastapi import UploadFile
import redis
from rq import Queue

from clusterization_engine.application import parser
from clusterization_engine.domain import custom, kmeans, sweep


OUTPUT_ROOT = Path("/app/output/clusterization_engine")
QUEUE_NAME = "clusterization_engine_jobs"


@dataclass
class ClusterizationParams:
    algoritmo: str
    sheet_name: str | None = None
    consultor_prefix: str = "Consultor"
    latitude_col: str | None = None
    longitude_col: str | None = None
    random_state: int = 42
    max_pdv_cluster: int = 200
    dias_uteis: int = 20
    freq: int = 1
    workday_min: int = 500
    route_km_max: float = 200.0
    service_min: int = 30
    v_kmh: float = 35.0
    alpha_path: float = 1.4
    max_iter: int = 10
    excluir_outliers: bool = False
    z_thresh: float = 3.0
    k_forcado: int | None = None


@dataclass
class JobState:
    job_id: str
    status: str
    progress: int
    message: str
    created_at: str
    updated_at: str
    params: dict[str, Any]
    input_path: str | None = None
    output_path: str | None = None
    error: str | None = None


def _now() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"


def get_redis_connection():
    redis_url = os.getenv("REDIS_URL") or (
        f"redis://{os.getenv('REDIS_HOST', 'redis')}:{os.getenv('REDIS_PORT', '6379')}/0"
    )
    return redis.from_url(redis_url)


def _job_key(job_id: str) -> str:
    return f"clusterization_engine:job:{job_id}"


def _save_job(job: JobState) -> None:
    get_redis_connection().set(_job_key(job.job_id), json.dumps(asdict(job)), ex=86400)


def _set_job(job_id: str, **updates: Any) -> None:
    job = get_job(job_id)
    if not job:
        return
    for key, value in updates.items():
        setattr(job, key, value)
    job.updated_at = _now()
    _save_job(job)


def get_job(job_id: str) -> JobState | None:
    raw = get_redis_connection().get(_job_key(job_id))
    if not raw:
        return None
    return JobState(**json.loads(raw))


def job_to_dict(job: JobState) -> dict[str, Any]:
    return asdict(job)


def _run_algorithm(df, params: ClusterizationParams):
    algoritmo = params.algoritmo.lower().strip()
    if algoritmo == "sweep":
        algoritmo = "capacitated_sweep"

    if algoritmo == "kmeans":
        return kmeans.clusterizar_kmeans(
            df,
            max_pdv_cluster=params.max_pdv_cluster,
            freq=params.freq,
            k_forcado=params.k_forcado,
            consultor_prefix=params.consultor_prefix,
            latitude_col=params.latitude_col,
            longitude_col=params.longitude_col,
            random_state=params.random_state,
        )
    if algoritmo == "capacitated_sweep":
        return sweep.clusterizar_sweep(
            df,
            max_pdv_cluster=params.max_pdv_cluster,
            consultor_prefix=params.consultor_prefix,
            latitude_col=params.latitude_col,
            longitude_col=params.longitude_col,
        )
    if algoritmo == "dense_subset":
        return custom.clusterizar_custom(
            df,
            max_pdv_cluster=params.max_pdv_cluster,
            consultor_prefix=params.consultor_prefix,
            latitude_col=params.latitude_col,
            longitude_col=params.longitude_col,
        )
    raise ValueError("Algoritmo invalido. Use: kmeans, capacitated_sweep, sweep ou dense_subset.")


def normalize_params(raw: dict[str, Any] | None, algoritmo: str | None = None) -> ClusterizationParams:
    data = dict(raw or {})
    if algoritmo:
        data["algoritmo"] = algoritmo
    if "algo" in data and "algoritmo" not in data:
        data["algoritmo"] = data.pop("algo")
    if "n_clusters" in data and "max_pdv_cluster" not in data:
        n_clusters = max(1, int(data.pop("n_clusters")))
        data["max_pdv_cluster"] = max(1, math.ceil(10**9 / n_clusters))
        data["k_forcado"] = n_clusters
    data.setdefault("algoritmo", "kmeans")
    allowed = {
        "algoritmo", "sheet_name", "consultor_prefix", "latitude_col", "longitude_col",
        "random_state", "max_pdv_cluster", "dias_uteis", "freq", "workday_min",
        "route_km_max", "service_min", "v_kmh", "alpha_path", "max_iter",
        "excluir_outliers", "z_thresh", "k_forcado",
    }
    clean = {k: v for k, v in data.items() if k in allowed}
    params = ClusterizationParams(**clean)
    validate_params(params)
    return params


def validate_params(params: ClusterizationParams) -> None:
    algo = params.algoritmo.lower().strip()
    if algo == "sweep":
        algo = "capacitated_sweep"
        params.algoritmo = algo
    if algo not in {"kmeans", "capacitated_sweep", "dense_subset"}:
        raise ValueError("Algoritmo invalido. Use: kmeans, capacitated_sweep, sweep ou dense_subset.")
    if params.max_pdv_cluster <= 0:
        raise ValueError("max_pdv_cluster deve ser maior que zero.")
    if params.dias_uteis <= 0:
        raise ValueError("dias_uteis deve ser maior que zero.")
    if params.freq <= 0:
        raise ValueError("freq deve ser maior que zero.")
    if params.workday_min <= 0:
        raise ValueError("workday_min deve ser maior que zero.")
    if params.route_km_max <= 0:
        raise ValueError("route_km_max deve ser maior que zero.")
    if params.service_min <= 0:
        raise ValueError("service_min deve ser maior que zero.")
    if params.v_kmh <= 0:
        raise ValueError("v_kmh deve ser maior que zero.")
    if params.alpha_path <= 0:
        raise ValueError("alpha_path deve ser maior que zero.")
    if params.max_iter <= 0:
        raise ValueError("max_iter deve ser maior que zero.")
    if params.z_thresh <= 0:
        raise ValueError("z_thresh deve ser maior que zero.")


async def create_job(file: UploadFile, params: ClusterizationParams) -> JobState:
    if not file.filename or not file.filename.lower().endswith(".xlsx"):
        raise ValueError("Envie um arquivo .xlsx.")

    job_id = str(uuid.uuid4())
    job_dir = OUTPUT_ROOT / job_id
    job_dir.mkdir(parents=True, exist_ok=True)
    input_path = job_dir / "input.xlsx"
    output_path = job_dir / "resultado_clusterizacao.xlsx"

    with input_path.open("wb") as destination:
        shutil.copyfileobj(file.file, destination)

    now = _now()
    state = JobState(
        job_id=job_id,
        status="queued",
        progress=0,
        message="Job enfileirado",
        created_at=now,
        updated_at=now,
        params=asdict(params),
        input_path=str(input_path),
        output_path=str(output_path),
    )
    _save_job(state)

    queue = Queue(QUEUE_NAME, connection=get_redis_connection())
    queue.enqueue(
        process_job,
        job_id,
        job_timeout=int(os.getenv("CLUSTERIZATION_ENGINE_JOB_TIMEOUT", "1800")),
        result_ttl=86400,
        failure_ttl=86400,
    )
    return state


def process_job(job_id: str) -> None:
    job = get_job(job_id)
    if not job or not job.input_path or not job.output_path:
        return

    params = ClusterizationParams(**job.params)
    sheet_name: str | int = 0 if not params.sheet_name else params.sheet_name
    start_time = time.time()

    try:
        _set_job(job_id, status="running", progress=10, message="Lendo planilha XLSX")
        with Path(job.input_path).open("rb") as input_file:
            df = parser.parse_entregas_planilha(input_file, sheet_name=sheet_name)

        if params.excluir_outliers:
            _set_job(job_id, progress=30, message="Detectando outliers geograficos")
            df = kmeans.remover_outliers_geograficos(
                df,
                latitude_col=params.latitude_col,
                longitude_col=params.longitude_col,
                z_thresh=params.z_thresh,
            )

        _set_job(job_id, progress=45, message="Processando clusterizacao")
        result = _run_algorithm(df, params)

        _set_job(job_id, progress=80, message="Gerando arquivo XLSX de saida")
        parser.salvar_output_planilha(result, job.output_path)

        elapsed = round(time.time() - start_time, 1)
        total_setores = result["consultor"].nunique() if "consultor" in result.columns else 0

        finished_job = get_job(job_id)
        if finished_job:
            finished_job.params = {
                **finished_job.params,
                "total_registros": len(result),
                "total_setores": total_setores,
                "tempo_execucao_s": elapsed,
            }
            finished_job.status = "finished"
            finished_job.progress = 100
            finished_job.message = "Clusterizacao concluida"
            finished_job.updated_at = _now()
            _save_job(finished_job)
    except Exception as exc:
        _set_job(
            job_id,
            status="failed",
            progress=100,
            message="Clusterizacao falhou",
            error=str(exc),
        )

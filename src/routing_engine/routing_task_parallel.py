# sales_router/src/routing_engine/routing_task_parallel.py

import json
import time
from rq import get_current_job

from routing_engine.infrastructure.queue_factory import get_queue
from routing_engine.application.consultor_service import ConsultorService
from routing_engine.application.balanced_subcluster_splitter import dividir_grupo_em_rotas_balanceadas
from routing_engine.application.route_optimizer import RouteOptimizer
from routing_engine.application.route_distance_service import RouteDistanceService
from routing_engine.domain.entities import RouteGroup, PDVData


# =========================================================
# 🔥 SUBJOB (1 GRUPO = 1 WORKER)
# =========================================================
def executar_routing_subjob(payload: dict):

    grupo_id = payload["grupo_id"]
    lista_pdvs = payload["pdvs"]
    params = payload["params"]

    # 🔒 isolamento total
    distance_service = RouteDistanceService()

    route_optimizer = RouteOptimizer(
        v_kmh=params.get("v_kmh", 60.0),
        service_min=params.get("service_min", 30.0),
        alpha_path=params.get("alpha_path", 1.3),
        distance_service=distance_service
    )

    consultor_service = ConsultorService(params.get("tenant_id", 1))

    consultor = str(grupo_id).strip().upper()
    base_lat, base_lon = consultor_service.get_base(consultor)

    pdvs = [PDVData(**p) for p in lista_pdvs]

    route_group = RouteGroup(
        group_id=grupo_id,
        group_type="consultor",
        centro_lat=base_lat,
        centro_lon=base_lon,
        n_pdvs=len(pdvs),
        pdvs=pdvs
    )

    result = dividir_grupo_em_rotas_balanceadas(
        route_group=route_group,
        dias_uteis=params.get("dias_uteis", 21),
        freq_padrao=params.get("freq_visita", 1),
        route_optimizer=route_optimizer,
        aplicar_two_opt=params.get("aplicar_two_opt", True),
        min_pdvs_rota=params.get("min_pdvs_rota", 8),
        max_pdvs_rota=params.get("max_pdvs_rota", 12),
    )
    stats = distance_service.get_stats()

    return {
        "grupo": result,
        "stats": stats
    }


# =========================================================
# 🚀 JOB PRINCIPAL (ORQUESTRADOR)
# =========================================================
def executar_routing_job(grupos_dict, params):

    job = get_current_job()

    queue = get_queue("routing_subjobs")

    jobs = []

    total = len(grupos_dict)
    enviados = 0

    # =====================================================
    # DISPARA SUBJOBS
    # =====================================================
    for grupo_id, lista_pdvs in grupos_dict.items():

        payload = {
            "grupo_id": grupo_id,
            "pdvs": [p.__dict__ for p in lista_pdvs],  # serializável
            "params": params
        }

        subjob = queue.enqueue(
            executar_routing_subjob,
            payload,
            job_timeout=3600
        )

        jobs.append(subjob)

        enviados += 1

        if job:
            job.meta["progress"] = int((enviados / total) * 30)
            job.meta["step"] = f"Disparando grupos ({enviados}/{total})"
            job.save_meta()

    # =====================================================
    # COLETA RESULTADOS
    # =====================================================
    resultados = []
    stats_list = []
    finalizados = 0

    while finalizados < total:

        finalizados = 0

        resultados_temp = []
        stats_temp = []

        for j in jobs:
            if j.is_finished:
                finalizados += 1

                if j.result and j.result.get("grupo"):
                    resultados_temp.append(j.result["grupo"])
                    stats_temp.append(j.result.get("stats", {}))

        # só atualiza quando tem resultado
        if resultados_temp:
            resultados = resultados_temp
            stats_list = stats_temp

        if job:
            job.meta["progress"] = 30 + int((finalizados / total) * 60)
            job.meta["step"] = f"Processando grupos ({finalizados}/{total})"
            job.save_meta()

        time.sleep(1)

    # =====================================================
    # AGREGA STATS
    # =====================================================
    stats_total = {
        "cache_hits": sum(s.get("cache_hits", 0) for s in stats_list),
        "osrm_hits": sum(s.get("osrm_hits", 0) for s in stats_list),
        "google_hits": sum(s.get("google_hits", 0) for s in stats_list),
        "haversine_hits": sum(s.get("haversine_hits", 0) for s in stats_list),
    }

    return resultados, stats_total
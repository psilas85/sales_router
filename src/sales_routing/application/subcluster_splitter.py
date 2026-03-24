# ============================================================
# 📦 src/sales_routing/application/subcluster_splitter.py
# ============================================================

import math
import numpy as np
from typing import List, Dict, Any
from sklearn.cluster import KMeans
from loguru import logger

from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData
from src.sales_routing.application.route_optimizer import RouteOptimizer


# ============================================================
# 🔹 Função principal — divisão adaptativa baseada em TEMPO REAL
# ============================================================
def dividir_cluster_em_subclusters(
    cluster: ClusterData,
    pdvs_cluster: List[PDVData],
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float,
    alpha_path: float = 1.3,
    aplicar_two_opt: bool = False
) -> Dict[str, Any]:

    if not pdvs_cluster:
        return {
            "cluster_id": cluster.cluster_id,
            "k_final": 0,
            "subclusters": []
        }

    optimizer = RouteOptimizer(
        v_kmh=v_kmh,
        service_min=service_min,
        alpha_path=alpha_path
    )

    k = 1
    convergiu = False
    resultados_iter = []

    while not convergiu:

        coords = np.array([[p.lat, p.lon] for p in pdvs_cluster])
        kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto").fit(coords)
        labels = kmeans.labels_

        subclusters = []
        max_tempo = 0.0
        max_dist = 0.0

        for sub_id in range(k):

            pdvs_sub = [p for i, p in enumerate(pdvs_cluster) if labels[i] == sub_id]

            if not pdvs_sub:
                continue

            centro = {
                "lat": cluster.centro_lat,
                "lon": cluster.centro_lon
            }

            pdvs_dict = [
                {"pdv_id": p.pdv_id, "lat": p.lat, "lon": p.lon}
                for p in pdvs_sub
            ]

            # 🔥 cálculo REAL da rota (OSRM)
            rota_result = optimizer.calcular_rota(
                centro,
                pdvs_dict,
                aplicar_two_opt=aplicar_two_opt
            )

            t_total = rota_result["tempo_total_min"]
            d_total = rota_result["distancia_total_km"]

            subclusters.append({
                "subcluster_id": sub_id + 1,
                "n_pdvs": len(pdvs_sub),
                "tempo_total_min": round(t_total, 1),
                "dist_total_km": round(d_total, 2),
                "pdvs": rota_result["sequencia"],
                "rota_coord": rota_result["rota_coord"],
            })

            max_tempo = max(max_tempo, t_total)
            max_dist = max(max_dist, d_total)

        resultados_iter.append((k, max_tempo, max_dist))

        # ====================================================
        # Critério de parada (tempo + distância)
        # ====================================================
        if max_tempo <= workday_min and max_dist <= route_km_max:
            convergiu = True
        else:
            k += 1
            if k > len(pdvs_cluster):
                logger.warning(
                    f"⚠️ Cluster {cluster.cluster_id}: não convergiu (k={k})"
                )
                break

    logger.success(
        f"✅ Cluster {cluster.cluster_id} | k_final={k} | "
        f"tempo_max={max_tempo:.1f} min | dist_max={max_dist:.1f} km"
    )

    return {
        "cluster_id": cluster.cluster_id,
        "k_final": k,
        "total_pdvs": len(pdvs_cluster),
        "max_tempo": round(max_tempo, 1),
        "max_dist": round(max_dist, 1),
        "iteracoes": resultados_iter,
        "subclusters": subclusters,
    }


# ============================================================
# 🔹 Pipeline geral
# ============================================================
def gerar_subclusters(
    clusters: List[ClusterData],
    pdvs: List[PDVData],
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float,
    alpha_path: float = 1.3,
    aplicar_two_opt: bool = False
) -> List[Dict[str, Any]]:

    resultados = []

    logger.info("🚀 Iniciando subclusterização adaptativa (tempo real)...")

    for cluster in clusters:

        pdvs_cluster = [
            p for p in pdvs if p.cluster_id == cluster.cluster_id
        ]

        if not pdvs_cluster:
            continue

        logger.info(
            f"\n📦 Cluster {cluster.cluster_id} ({len(pdvs_cluster)} PDVs)"
        )

        resultado = dividir_cluster_em_subclusters(
            cluster=cluster,
            pdvs_cluster=pdvs_cluster,
            workday_min=workday_min,
            route_km_max=route_km_max,
            service_min=service_min,
            v_kmh=v_kmh,
            alpha_path=alpha_path,
            aplicar_two_opt=aplicar_two_opt
        )

        resultados.append(resultado)

    logger.success(f"🏁 Subclusterização concluída ({len(resultados)} clusters)")

    return resultados
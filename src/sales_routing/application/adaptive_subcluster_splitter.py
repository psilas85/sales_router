# src/sales_routing/application/adaptive_subcluster_splitter.py

import math
import numpy as np
from sklearn.cluster import KMeans
from typing import List, Dict, Any
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData
from src.sales_routing.application.route_optimizer import RouteOptimizer


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Distância Haversine em km entre dois pontos."""
    R = 6371.0
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def dividir_cluster_em_subclusters(
    cluster: ClusterData,
    pdvs_cluster: List[PDVData],
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float,
    alpha_path: float,
    aplicar_two_opt: bool = False
) -> Dict[str, Any]:
    """
    Divide adaptativamente um cluster em subclusters até que o tempo máximo e distância sejam respeitados.
    Usa heurísticas de roteirização para avaliar a viabilidade de cada subcluster.
    """

    k = 1
    convergiu = False
    resultados_iter = []
    optimizer = RouteOptimizer(v_kmh=v_kmh, service_min=service_min, alpha_path=alpha_path)

    while not convergiu:
        coords = np.array([[p.lat, p.lon] for p in pdvs_cluster])
        kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto").fit(coords)
        labels = kmeans.labels_

        subclusters = []
        max_tempo = 0.0
        max_dist = 0.0

        for subcluster_id in range(k):
            pdvs_sub = [p for i, p in enumerate(pdvs_cluster) if labels[i] == subcluster_id]
            if not pdvs_sub:
                continue

            # Constrói lista simples de dicts para o otimizador
            pdvs_dict = [{"pdv_id": p.pdv_id, "lat": p.lat, "lon": p.lon} for p in pdvs_sub]
            centro = {"lat": cluster.centro_lat, "lon": cluster.centro_lon}

            # Calcula rota otimizada
            rota = optimizer.calcular_rota(centro, pdvs_dict, aplicar_two_opt)

            subclusters.append({
                "subcluster_id": subcluster_id + 1,
                "n_pdvs": len(pdvs_sub),
                "tempo_total_min": rota["tempo_total_min"],
                "dist_total_km": rota["distancia_total_km"],
                "pdvs": [p["pdv_id"] for p in rota["sequencia"]],
            })

            max_tempo = max(max_tempo, rota["tempo_total_min"])
            max_dist = max(max_dist, rota["distancia_total_km"])

        resultados_iter.append((k, max_tempo, max_dist))

        if max_tempo <= workday_min and max_dist <= route_km_max:
            convergiu = True
        else:
            k += 1
            if k > len(pdvs_cluster):
                print(f"⚠️ Cluster {cluster.cluster_id}: não convergiu — limite de PDVs atingido ({len(pdvs_cluster)}).")
                break

    print(f"  ✅ K_final={k}, MáxTempo={max_tempo:.1f} min, MáxDist={max_dist:.1f} km")
    print(f"  🧩 Iterações: {[f'K={k_},T={t:.1f}m,D={d:.1f}km' for k_, t, d in resultados_iter]}")

    return {
        "cluster_id": cluster.cluster_id,
        "k_final": k,
        "total_pdvs": len(pdvs_cluster),
        "max_tempo": round(max_tempo, 1),
        "max_dist": round(max_dist, 1),
        "iteracoes": resultados_iter,
        "subclusters": subclusters,
    }


def gerar_subclusters_adaptativo(
    clusters: List[ClusterData],
    pdvs: List[PDVData],
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float,
    alpha_path: float,
    aplicar_two_opt: bool = False
) -> List[Dict[str, Any]]:
    """
    Gera subclusters adaptativamente para todos os clusters.
    """

    resultados = []

    for cluster in clusters:
        pdvs_cluster = [p for p in pdvs if p.cluster_id == cluster.cluster_id]
        if not pdvs_cluster:
            continue

        print(f"\n🧭 Cluster {cluster.cluster_id} → {len(pdvs_cluster)} PDVs")
        resultado = dividir_cluster_em_subclusters(
            cluster,
            pdvs_cluster,
            workday_min,
            route_km_max,
            service_min,
            v_kmh,
            alpha_path,
            aplicar_two_opt
        )

        resultados.append(resultado)

    return resultados

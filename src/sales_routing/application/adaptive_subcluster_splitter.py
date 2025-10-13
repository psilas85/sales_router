# src/sales_routing/application/adaptive_subcluster_splitter.py

import math
import numpy as np
from sklearn.cluster import KMeans
from typing import List, Dict, Any
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Distância Haversine em km entre dois pontos."""
    R = 6371.0
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


def tempo_total_estimado(pdvs: List[PDVData], service_min: int, v_kmh: float, alpha: float) -> tuple[float, float]:
    """Calcula o tempo total e distância total aproximada para uma rota (min, km)."""
    if len(pdvs) <= 1:
        return service_min, 0.0

    total_dist = 0.0
    for i in range(len(pdvs) - 1):
        total_dist += haversine_km(
            (pdvs[i].lat, pdvs[i].lon),
            (pdvs[i + 1].lat, pdvs[i + 1].lon)
        )

    # Corrige distâncias para tortuosidade de vias
    total_dist *= alpha

    tempo_viagem_min = (total_dist / v_kmh) * 60
    tempo_servico_min = len(pdvs) * service_min
    tempo_total = tempo_viagem_min + tempo_servico_min

    return tempo_total, total_dist


def dividir_cluster_em_subclusters(
    cluster: ClusterData,
    pdvs_cluster: List[PDVData],
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float,
    alpha_path: float
) -> Dict[str, Any]:
    """
    Divide adaptativamente um cluster em subclusters até que o tempo máximo e distância sejam respeitados.
    """

    # Ponto de partida: tenta com K=1
    k = 1
    convergiu = False
    resultados_iter = []

    while not convergiu:
        # Aplica KMeans para subdividir PDVs em K subclusters
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

            t_total, d_total = tempo_total_estimado(pdvs_sub, service_min, v_kmh, alpha_path)
            subclusters.append({
                "subcluster_id": subcluster_id + 1,
                "n_pdvs": len(pdvs_sub),
                "tempo_total_min": round(t_total, 1),
                "dist_total_km": round(d_total, 2),
                "pdvs": [p.pdv_id for p in pdvs_sub]
            })

            max_tempo = max(max_tempo, t_total)
            max_dist = max(max_dist, d_total)

        resultados_iter.append((k, max_tempo, max_dist))

        if max_tempo <= workday_min and max_dist <= route_km_max:
            convergiu = True
        else:
            k += 1
            if k > len(pdvs_cluster):
                # Evita loop infinito
                print(f"⚠️ Cluster {cluster.cluster_id}: número de PDVs insuficiente para satisfazer restrições.")
                break

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
    alpha_path: float
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
            alpha_path
        )

        print(f"  ✅ K_final={resultado['k_final']}, MáxTempo={resultado['max_tempo']:.1f} min, MáxDist={resultado['max_dist']:.1f} km")
        print(f"  🧩 Iterações: {[f'K={k},T={t:.1f}m,D={d:.1f}km' for k,t,d in resultado['iteracoes']]}")

        resultados.append(resultado)

    return resultados

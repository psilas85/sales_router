# src/sales_routing/application/subcluster_splitter.py

import math
from typing import List, Dict
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData


def haversine_km(lat1, lon1, lat2, lon2) -> float:
    """Calcula distância em km entre dois pontos (Haversine)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2) + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * (math.sin(dlon / 2) ** 2)
    return 2 * R * math.asin(math.sqrt(a))


def calcular_tempo_total(dist_km: float, n_pdvs: int, service_min: int, v_kmh: float) -> float:
    """Tempo total estimado (minutos) = deslocamento + serviço."""
    tempo_viagem = (dist_km / v_kmh) * 60
    tempo_servico = n_pdvs * service_min
    return tempo_viagem + tempo_servico


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
    Cada subcluster representará uma rota diária viável para um vendedor.
    """

    # Ponto de partida — tenta com K=1 (toda a área em uma rota só)
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

            # Calcula tempo e distância estimados para esse subconjunto de PDVs
            t_total, d_total = tempo_total_estimado(pdvs_sub, service_min, v_kmh, alpha_path)

            # Cria uma estrutura de subcluster (rota)
            subclusters.append({
                "subcluster_id": subcluster_id + 1,
                "n_pdvs": len(pdvs_sub),
                "tempo_total_min": round(t_total, 1),
                "dist_total_km": round(d_total, 2),
                "pdvs": [p.pdv_id for p in pdvs_sub],  # garante que são objetos PDVData
            })

            # Atualiza métricas máximas de tempo e distância
            max_tempo = max(max_tempo, t_total)
            max_dist = max(max_dist, d_total)

        resultados_iter.append((k, max_tempo, max_dist))

        # Critério de parada
        if max_tempo <= workday_min and max_dist <= route_km_max:
            convergiu = True
        else:
            k += 1
            if k > len(pdvs_cluster):
                print(f"⚠️ Cluster {cluster.cluster_id}: não convergiu — limite de PDVs atingido ({len(pdvs_cluster)}).")
                break

    # Registro final do cluster
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


def gerar_subclusters(
    clusters: List[ClusterData],
    pdvs: List[PDVData],
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float
) -> List[Dict]:
    """Aplica a divisão de subclusters para todos os clusters."""
    resultados = []
    for cluster in clusters:
        pdvs_cluster = [p for p in pdvs if p.cluster_id == cluster.cluster_id]
        if not pdvs_cluster:
            continue
        subclusters = dividir_em_subclusters(cluster, pdvs_cluster, workday_min, route_km_max, service_min, v_kmh)
        resultados.extend(subclusters)
    return resultados

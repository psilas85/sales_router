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


def dividir_em_subclusters(
    cluster: ClusterData,
    pdvs: List[PDVData],
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float
) -> List[Dict]:
    """
    Divide um cluster em subclusters (rotas diárias).
    Cada subcluster respeita limite de tempo e distância.
    """

    subclusters = []
    pdvs_restantes = pdvs.copy()
    subcluster_id = 1

    while pdvs_restantes:
        base = pdvs_restantes[0]
        grupo = [base]
        total_dist = 0.0
        total_tempo = service_min

        for candidato in pdvs_restantes[1:]:
            dist_km = haversine_km(base.lat, base.lon, candidato.lat, candidato.lon)
            tempo_estimado = calcular_tempo_total(total_dist + dist_km, len(grupo) + 1, service_min, v_kmh)

            if tempo_estimado <= workday_min and (total_dist + dist_km) <= route_km_max:
                grupo.append(candidato)
                total_dist += dist_km
                total_tempo = tempo_estimado
            else:
                # rota cheia
                break

        # Adiciona o subcluster finalizado
        subclusters.append({
            "cluster_id": cluster.cluster_id,
            "subcluster_label": subcluster_id,
            "n_pdvs": len(grupo),
            "pdvs": [p.pdv_id for p in grupo],
            "distancia_total_km": round(total_dist, 2),
            "tempo_total_min": round(total_tempo, 1),
        })

        # Remove os PDVs usados
        ids_grupo = {p.pdv_id for p in grupo}
        pdvs_restantes = [p for p in pdvs_restantes if p.pdv_id not in ids_grupo]

        subcluster_id += 1

    return subclusters


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

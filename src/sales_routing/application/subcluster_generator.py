# ============================================================
# 📦 src/sales_routing/application/subcluster_generator.py
# ============================================================

import math
import numpy as np
from typing import List, Dict, Any
from sklearn.cluster import KMeans
from loguru import logger
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData
from src.sales_routing.application.route_optimizer import RouteOptimizer


def calcular_rotas_diarias_por_frequencia(dias_uteis: int, frequencia_visita: int) -> int:
    """
    Calcula o número de rotas (subclusters) mensais possíveis para um vendedor
    com base nos dias úteis e na frequência de visita ao PDV.
      Exemplo:
        - 21 dias úteis, frequência 1x/mês → 21 rotas
        - 21 dias úteis, frequência 2x/mês → 10 (21/2 ≈ 10)
    """
    if frequencia_visita <= 0:
        raise ValueError("❌ frequencia_visita deve ser >= 1")
    rotas = max(1, math.floor(dias_uteis / frequencia_visita))
    logger.info(f"📆 Rotas mensais calculadas: {rotas} (dias_uteis={dias_uteis}, frequencia={frequencia_visita})")
    return rotas


def dividir_cluster_por_capacidade(
    cluster: ClusterData,
    pdvs_cluster: List[PDVData],
    dias_uteis: int,
    frequencia_visita: int,
    route_optimizer: RouteOptimizer
) -> Dict[str, Any]:
    """
    Divide um cluster em subclusters diários com base na capacidade de rotas mensais.
    Cada subcluster será uma rota diária partindo e retornando ao centro do cluster.
    """

    n_rotas = calcular_rotas_diarias_por_frequencia(dias_uteis, frequencia_visita)
    if n_rotas > len(pdvs_cluster):
        n_rotas = len(pdvs_cluster)

    coords = np.array([[p.lat, p.lon] for p in pdvs_cluster])
    kmeans = KMeans(n_clusters=n_rotas, random_state=42, n_init="auto").fit(coords)
    labels = kmeans.labels_

    subclusters = []
    for sub_id in range(n_rotas):
        pdvs_sub = [p for i, p in enumerate(pdvs_cluster) if labels[i] == sub_id]
        if not pdvs_sub:
            continue

        centro = {"lat": cluster.centro_lat, "lon": cluster.centro_lon}
        pdvs_dict = [{"pdv_id": p.pdv_id, "lat": p.lat, "lon": p.lon} for p in pdvs_sub]

        rota_result = route_optimizer.calcular_rota(centro, pdvs_dict)

        subclusters.append({
            "subcluster_id": sub_id + 1,
            "n_pdvs": len(pdvs_sub),
            "tempo_total_min": rota_result["tempo_total_min"],
            "dist_total_km": rota_result["distancia_total_km"],
            "pdvs": rota_result["sequencia"],
            "rota_coord": rota_result["rota_coord"],
        })

        logger.info(
            f"🧭 Subcluster {sub_id+1}/{n_rotas} | {len(pdvs_sub)} PDVs | "
            f"{rota_result['distancia_total_km']:.1f} km | {rota_result['tempo_total_min']:.1f} min"
        )

    return {
        "cluster_id": cluster.cluster_id,
        "k_final": n_rotas,
        "total_pdvs": len(pdvs_cluster),
        "subclusters": subclusters,
    }


def gerar_subclusters_por_capacidade(
    clusters: List[ClusterData],
    pdvs: List[PDVData],
    dias_uteis: int,
    frequencia_visita: int,
    v_kmh: float = 35.0,
    service_min: int = 15,
    alpha_path: float = 1.3,
    aplicar_two_opt: bool = False
) -> List[Dict[str, Any]]:
    """
    Pipeline principal para gerar subclusters (rotas diárias) com base em capacidade mensal.
    """
    resultados = []
    optimizer = RouteOptimizer(v_kmh=v_kmh, service_min=service_min, alpha_path=alpha_path)

    logger.info(
        f"🚀 Gerando subclusters com base em capacidade mensal: "
        f"{dias_uteis} dias úteis / {frequencia_visita}x por mês"
    )

    for cluster in clusters:
        pdvs_cluster = [p for p in pdvs if p.cluster_id == cluster.cluster_id]
        if not pdvs_cluster:
            continue

        logger.info(f"\n📦 Cluster {cluster.cluster_id} ({len(pdvs_cluster)} PDVs) → Dividindo em rotas diárias...")
        resultado = dividir_cluster_por_capacidade(
            cluster, pdvs_cluster, dias_uteis, frequencia_visita, optimizer
        )
        resultados.append(resultado)

    logger.success(f"✅ Subclusterização concluída ({len(resultados)} clusters processados)")
    return resultados

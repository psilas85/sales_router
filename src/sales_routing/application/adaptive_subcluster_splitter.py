#sales_router/src/sales_routing/application/adaptive_subcluster_splitter.py

# ============================================================
# 📦 sales_router/src/sales_routing/application/adaptive_subcluster_splitter.py
# ============================================================

import math
import numpy as np
from sklearn.cluster import KMeans
from typing import List, Dict, Any
from loguru import logger
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData
from src.sales_routing.application.route_optimizer import RouteOptimizer


# ============================================================
# 🔹 Função auxiliar: distância Haversine
# ============================================================
def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    R = 6371.0
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


# ============================================================
# 🔹 Sequenciamento: Nearest Neighbor (com 2-Opt opcional)
# ============================================================
def nearest_neighbor_sequence(centro: dict, pdvs: List[dict]) -> List[dict]:
    """Ordena os PDVs pela heurística do vizinho mais próximo, partindo do centro do cluster."""
    visitados = []
    restantes = pdvs.copy()
    atual = {"lat": centro["lat"], "lon": centro["lon"]}
    while restantes:
        prox = min(restantes, key=lambda p: haversine_km(
            (atual["lat"], atual["lon"]), (p["lat"], p["lon"])
        ))
        visitados.append(prox)
        restantes.remove(prox)
        atual = {"lat": prox["lat"], "lon": prox["lon"]}
    return visitados


def total_dist_haversine(rota: List[dict]) -> float:
    if len(rota) < 2:
        return 0.0
    return sum(
        haversine_km(
            (rota[i]["lat"], rota[i]["lon"]),
            (rota[i + 1]["lat"], rota[i + 1]["lon"])
        )
        for i in range(len(rota) - 1)
    )


def two_opt(rota: List[dict]) -> List[dict]:
    """Refina a sequência trocando pares de arestas para reduzir a distância total."""
    if len(rota) < 4:
        return rota

    melhor_rota = rota
    melhor_dist = total_dist_haversine(rota)
    melhorou = True

    while melhorou:
        melhorou = False
        for i in range(1, len(rota) - 2):
            for j in range(i + 1, len(rota)):
                if j - i == 1:
                    continue
                nova_rota = rota[:i] + rota[i:j][::-1] + rota[j:]
                nova_dist = total_dist_haversine(nova_rota)
                if nova_dist + 0.001 < melhor_dist:
                    melhor_rota, melhor_dist = nova_rota, nova_dist
                    melhorou = True
        rota = melhor_rota
    return melhor_rota


# ============================================================
# 🔹 Divisão de cluster por frequência e dias úteis
# ============================================================
def dividir_cluster_em_subclusters(
    cluster: ClusterData,
    pdvs_cluster: List[PDVData],
    dias_uteis: int,
    freq_padrao: float,
    v_kmh: float,
    service_min: int,
    alpha_path: float = 1.3,
    aplicar_two_opt: bool = False
) -> Dict[str, Any]:
    """
    Divide o cluster em subclusters diários com base em frequência de visita e dias úteis.
    Agora inclui geração real de rota (rota_coord) via RouteOptimizer.
    """

    # ======================================================
    # 1️⃣ Calcular frequência ponderada e subclusters necessários
    # ======================================================
    for p in pdvs_cluster:
        p.freq_visita = getattr(p, "freq_visita", freq_padrao)

    # roteiros = floor(dias_uteis / frequencia)
    # ex: 21/2 -> 10; 21/3 -> 7
    freq = max(1, int(freq_padrao or 1))
    n_subclusters = max(1, dias_uteis // freq)
    n_subclusters = min(n_subclusters, len(pdvs_cluster))

    logger.info(
        f"📅 Cluster {cluster.cluster_id}: {len(pdvs_cluster)} PDVs | "
        f"freq={freq}x/mês | dias_uteis={dias_uteis} → {n_subclusters} roteiros base"
    )

    # ======================================================
    # 2️⃣ Clusterização espacial (1 subcluster = 1 dia útil)
    # ======================================================
    coords = np.array([[p.lat, p.lon] for p in pdvs_cluster])
    kmeans = KMeans(n_clusters=n_subclusters, random_state=42, n_init="auto").fit(coords)
    labels = kmeans.labels_

    # 💡 Agora passando alpha_path explicitamente
    optimizer = RouteOptimizer(v_kmh=v_kmh, service_min=service_min, alpha_path=alpha_path)

    subclusters = []
    for sub_id in range(n_subclusters):
        pdvs_sub = [p for i, p in enumerate(pdvs_cluster) if labels[i] == sub_id]
        if not pdvs_sub:
            continue

        centro = {"lat": cluster.centro_lat, "lon": cluster.centro_lon}
        pdvs_dict = [{"pdv_id": p.pdv_id, "lat": p.lat, "lon": p.lon} for p in pdvs_sub]

        rota_result = optimizer.calcular_rota(centro, pdvs_dict, aplicar_two_opt=aplicar_two_opt)

        subclusters.append({
            "subcluster_id": sub_id + 1,
            "n_pdvs": len(pdvs_sub),
            "tempo_total_min": rota_result["tempo_total_min"],
            "dist_total_km": rota_result["distancia_total_km"],
            "pdvs": rota_result["sequencia"],
            "rota_coord": rota_result["rota_coord"],
        })

        logger.info(
            f"🧭 Subcluster {sub_id+1}/{n_subclusters} | "
            f"{len(pdvs_sub)} PDVs | {rota_result['distancia_total_km']:.1f} km | "
            f"{rota_result['tempo_total_min']:.1f} min"
        )

    # ======================================================
    # 3️⃣ Consolidação mensal
    # ======================================================
    tempo_total_mes = sum(s["tempo_total_min"] for s in subclusters)
    dist_total_mes = sum(s["dist_total_km"] for s in subclusters)
    pdvs_medio = np.mean([s["n_pdvs"] for s in subclusters]) if subclusters else 0

    logger.success(
        f"✅ Cluster {cluster.cluster_id}: {n_subclusters} rotas | "
        f"Tempo total mês={tempo_total_mes:.1f} min | Distância total mês={dist_total_mes:.1f} km | "
        f"PDVs médios={pdvs_medio:.1f}"
    )

    return {
        "cluster_id": cluster.cluster_id,
        "k_final": n_subclusters,        # <-- adiciona isso
        "total_pdvs": len(pdvs_cluster), # <-- opcional, mas bom
        "subclusters": subclusters,
        # pode manter os campos extras se quiser
        "tempo_total_mes": tempo_total_mes,
        "dist_total_mes": dist_total_mes,
        "mean_pdvs": round(pdvs_medio, 1),
    }


# ============================================================
# 🔹 Pipeline geral de geração de rotas
# ============================================================
def gerar_subclusters_adaptativo(
    clusters: List[ClusterData],
    pdvs: List[PDVData],
    dias_uteis: int,
    freq_padrao: float,
    v_kmh: float,
    service_min: int,
    alpha_path: float = 1.3,
    aplicar_two_opt: bool = False
) -> List[Dict[str, Any]]:
    """
    Executa a subclusterização de todos os clusters,
    considerando frequência e dias úteis, com rotas completas.
    """
    resultados = []
    logger.info("🚀 Iniciando subclusterização baseada em frequência e dias úteis...")

    for cluster in clusters:
        pdvs_cluster = [p for p in pdvs if p.cluster_id == cluster.cluster_id]
        if not pdvs_cluster:
            logger.warning(f"⚠️ Cluster {cluster.cluster_id} sem PDVs — ignorado.")
            continue

        logger.info(f"\n🧭 Cluster {cluster.cluster_id} → {len(pdvs_cluster)} PDVs")
        resultado = dividir_cluster_em_subclusters(
            cluster=cluster,
            pdvs_cluster=pdvs_cluster,
            dias_uteis=dias_uteis,
            freq_padrao=freq_padrao,
            v_kmh=v_kmh,
            service_min=service_min,
            alpha_path=alpha_path,
            aplicar_two_opt=aplicar_two_opt,
        )
        resultados.append(resultado)

    logger.success(f"🏁 Subclusterização concluída para {len(resultados)} clusters.")
    return resultados

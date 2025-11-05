#sales_router/src/sales_routing/application/fixed_subcluster_splitter.py

# ============================================================
# 📦 src/sales_routing/application/fixed_subcluster_splitter.py
# ============================================================

import math
import numpy as np
from typing import List, Dict, Any
from loguru import logger
from sklearn.cluster import KMeans
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData
from src.sales_routing.application.route_optimizer import RouteOptimizer


# ============================================================
# 🔹 Subclusterização fixa por KMeans (sem adaptação)
# ============================================================
def dividir_cluster_em_subclusters_fixos(
    cluster: ClusterData,
    pdvs_cluster: List[PDVData],
    dias_uteis: int,
    freq_padrao: float,
    v_kmh: float,
    service_min: int,
    alpha_path: float = 1.3,
    aplicar_two_opt: bool = False,
    modo_calculo: str = "proporcional",  # 👈 NOVO
) -> Dict[str, Any]:
    """
    Divide um cluster macro em subclusters (rotas diárias) via KMeans,
    com base em dias úteis e frequência de visitas.
    🔸 modo_calculo='proporcional':  n = ceil(visitas_totais / dias_uteis)
    🔸 modo_calculo='fixo':          n = dias_uteis
    """

    for p in pdvs_cluster:
        p.freq_visita = getattr(p, "freq_visita", freq_padrao)

    visitas_totais = sum(p.freq_visita for p in pdvs_cluster)

    # ======================================================
    # 1️⃣ Determinar número de subclusters conforme modo
    # ======================================================
    if modo_calculo == "fixo":
        # ✅ Fixamos o número exato de subclusters = dias_uteis × freq_padrao
        n_subclusters = max(1, int(dias_uteis * freq_padrao))
        logger.info(
            f"📦 Cluster {cluster.cluster_id}: modo_calculo=fixo → {n_subclusters} rotas "
            f"(dias_uteis={dias_uteis}, freq_padrao={freq_padrao})"
        )
    else:
        # ✅ Modo proporcional mantém a lógica atual (número ajustado conforme total de visitas)
        n_subclusters = max(1, math.ceil(sum(p.freq_visita for p in pdvs_cluster) / dias_uteis))
        logger.info(
            f"📦 Cluster {cluster.cluster_id}: modo_calculo=proporcional → "
            f"{len(pdvs_cluster)} PDVs | visitas={sum(p.freq_visita for p in pdvs_cluster):.1f} → "
            f"{n_subclusters} rotas (dias_uteis={dias_uteis}, freq={freq_padrao})"
        )

    # ⚠️ Garantia: nunca mais rotas do que PDVs
    if n_subclusters > len(pdvs_cluster):
        n_subclusters = len(pdvs_cluster)
        logger.warning(f"⚠️ Ajustado número de subclusters para {n_subclusters} (máx PDVs = {len(pdvs_cluster)})")

    # ======================================================
    # 2️⃣ Clusterização espacial (KMeans fixo)
    # ======================================================
    coords = np.array([[p.lat, p.lon] for p in pdvs_cluster])
    kmeans = KMeans(n_clusters=n_subclusters, random_state=42, n_init="auto").fit(coords)
    labels = kmeans.labels_

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
            "centro_lat": cluster.centro_lat,   # ✅ novo campo
            "centro_lon": cluster.centro_lon,   # ✅ novo campo
        })


        logger.info(
            f"🧭 Subcluster {sub_id+1}/{n_subclusters} | "
            f"{len(pdvs_sub)} PDVs | {rota_result['distancia_total_km']:.1f} km | "
            f"{rota_result['tempo_total_min']:.1f} min"
        )

    # ======================================================
    # 3️⃣ Consolidação do cluster
    # ======================================================
    tempo_total_mes = sum(s["tempo_total_min"] for s in subclusters)
    dist_total_mes = sum(s["dist_total_km"] for s in subclusters)
    pdvs_medio = np.mean([s["n_pdvs"] for s in subclusters]) if subclusters else 0

    logger.success(
        f"✅ Cluster {cluster.cluster_id}: {n_subclusters} rotas fixas | "
        f"Tempo total mês={tempo_total_mes:.1f} min | "
        f"Distância total mês={dist_total_mes:.1f} km | PDVs médios={pdvs_medio:.1f}"
    )

    return {
        "cluster_id": cluster.cluster_id,
        "n_subclusters": n_subclusters,
        "tempo_total_mes": tempo_total_mes,
        "dist_total_mes": dist_total_mes,
        "mean_pdvs": round(pdvs_medio, 1),
        "subclusters": subclusters,
    }


# ============================================================
# 🔹 Pipeline geral de subclusterização fixa
# ============================================================
def gerar_subclusters_fixos(
    clusters: List[ClusterData],
    pdvs: List[PDVData],
    dias_uteis: int,
    freq_padrao: float,
    v_kmh: float,
    service_min: int,
    alpha_path: float = 1.3,
    aplicar_two_opt: bool = False,
    modo_calculo: str = "proporcional",  # 👈 NOVO
) -> List[Dict[str, Any]]:
    """
    Executa subclusterização fixa para todos os clusters (sem iteração adaptativa).
    Cada cluster principal é dividido em rotas fixas baseadas em KMeans.
    """
    resultados = []
    logger.info("🚀 Iniciando subclusterização fixa por KMeans (sem adaptação)...")

    for cluster in clusters:
        pdvs_cluster = [p for p in pdvs if p.cluster_id == cluster.cluster_id]
        if not pdvs_cluster:
            logger.warning(f"⚠️ Cluster {cluster.cluster_id} sem PDVs — ignorado.")
            continue

        logger.info(f"\n🧭 Processando Cluster {cluster.cluster_id} ({len(pdvs_cluster)} PDVs)")
        resultado = dividir_cluster_em_subclusters_fixos(
            cluster=cluster,
            pdvs_cluster=pdvs_cluster,
            dias_uteis=dias_uteis,
            freq_padrao=freq_padrao,
            v_kmh=v_kmh,
            service_min=service_min,
            alpha_path=alpha_path,
            aplicar_two_opt=aplicar_two_opt,
            modo_calculo=modo_calculo,  # 👈 propaga
        )
        resultados.append(resultado)

    logger.success(f"🏁 Subclusterização fixa concluída para {len(resultados)} clusters.")
    return resultados

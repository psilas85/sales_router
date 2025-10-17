# ============================================================
# 📦 sales_router/src/sales_routing/application/adaptive_subcluster_splitter.py
# ============================================================

import math
import numpy as np
import os
from sklearn.cluster import KMeans
from typing import List, Dict, Any
from datetime import datetime
from geopy.distance import geodesic
from loguru import logger
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData

# ============================================================
# ⚙️ PARÂMETROS GLOBAIS AJUSTÁVEIS
# ============================================================
K_DIVISOR = int(os.getenv("K_DIVISOR", 24))                   # quanto menor → mais subclusters iniciais

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
# 🔹 Nearest Neighbor (heurística rápida)
# ============================================================
def nearest_neighbor_sequence(centro: dict, pdvs: List[dict]) -> List[dict]:
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


# ============================================================
# 🔹 Fusão de subclusters curtos (por quantidade de PDVs)
# ============================================================
def merge_small_routes(subclusters, min_pdvs: int = 5):
    """Funde apenas subclusters com até `min_pdvs` PDVs ao mais próximo."""
    pequenos = [s for s in subclusters if s["n_pdvs"] < min_pdvs]
    grandes = [s for s in subclusters if s["n_pdvs"] >= min_pdvs]

    if not pequenos or not grandes:
        return subclusters

    logger.info(f"🔧 Fundindo {len(pequenos)} subclusters com <{min_pdvs} PDVs...")
    for s in pequenos:
        centro_s = (
            np.mean([p["lat"] for p in s["pdvs"]]),
            np.mean([p["lon"] for p in s["pdvs"]])
        )
        destino = min(grandes, key=lambda g: geodesic(
            centro_s,
            (
                np.mean([p["lat"] for p in g["pdvs"]]),
                np.mean([p["lon"] for p in g["pdvs"]])
            )
        ).km)

        destino["pdvs"].extend(s["pdvs"])
        destino["n_pdvs"] += s["n_pdvs"]
        destino["tempo_total_min"] += s["tempo_total_min"]
        destino["dist_total_km"] += s["dist_total_km"]

    logger.success(f"✅ {len(pequenos)} subclusters pequenos fundidos ao mais próximo.")
    return [s for s in grandes]


# ============================================================
# 🔹 Consolidação de unitários
# ============================================================
def consolidar_unitarios(subclusters, cluster, workday_min, route_km_max, service_min, v_kmh):
    unitarios = [s for s in subclusters if s["n_pdvs"] == 1]
    if not unitarios:
        return subclusters, 0

    logger.info(f"⚙️ Ajustando subclusters unitários ({len(unitarios)} detectados)...")
    fundidos = 0
    for sub_unit in unitarios:
        pdv = sub_unit["pdvs"][0]
        candidatos = [s for s in subclusters if s["n_pdvs"] > 1]
        if not candidatos:
            continue

        candidatos.sort(
            key=lambda s: haversine_km(
                (pdv["lat"], pdv["lon"]),
                (
                    np.mean([p["lat"] for p in s["pdvs"]]),
                    np.mean([p["lon"] for p in s["pdvs"]])
                )
            )
        )

        for destino in candidatos:
            dist_extra = haversine_km(
                (pdv["lat"], pdv["lon"]),
                (
                    np.mean([p["lat"] for p in destino["pdvs"]]),
                    np.mean([p["lon"] for p in destino["pdvs"]])
                )
            )
            novo_tempo = destino["tempo_total_min"] + service_min
            nova_dist = destino["dist_total_km"] + dist_extra
            if novo_tempo <= workday_min and nova_dist <= route_km_max:
                destino["pdvs"].append(pdv)
                destino["n_pdvs"] += 1
                destino["tempo_total_min"] = novo_tempo
                destino["dist_total_km"] = nova_dist
                subclusters.remove(sub_unit)
                fundidos += 1
                break

    if fundidos > 0:
        logger.success(f"✅ {fundidos} subclusters unitários fundidos.")
    else:
        logger.info("ℹ️ Nenhum subcluster unitário pôde ser consolidado.")
    return subclusters, fundidos


# ============================================================
# 🔹 Divisão adaptativa com convergência teórica
# ============================================================
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

    k = max(1, int(len(pdvs_cluster) / K_DIVISOR))
    convergiu = False
    violacoes = []

    diagnostic_log = "/tmp/diagnostic_pre_osrm.csv"
    os.makedirs(os.path.dirname(diagnostic_log), exist_ok=True)
    if not os.path.exists(diagnostic_log):
        with open(diagnostic_log, "w", encoding="utf-8") as f:
            f.write("timestamp,cluster_id,subcluster_id,tempo_teorico_min,distancia_km,n_pdvs\n")

    while not convergiu:
        coords = np.array([[p.lat, p.lon] for p in pdvs_cluster])
        kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto").fit(coords)
        labels = kmeans.labels_

        subclusters, tempos_sub, dist_sub, pdvs_count = [], [], [], []
        violacoes.clear()

        for sub_id in range(k):
            pdvs_sub = [p for i, p in enumerate(pdvs_cluster) if labels[i] == sub_id]
            if not pdvs_sub:
                continue

            centro = {"lat": cluster.centro_lat, "lon": cluster.centro_lon}
            pdvs_dict = [{"pdv_id": p.pdv_id, "lat": p.lat, "lon": p.lon} for p in pdvs_sub]
            pdvs_ordenados = nearest_neighbor_sequence(centro, pdvs_dict)

            total_km = sum(
                haversine_km(
                    (pdvs_ordenados[i]["lat"], pdvs_ordenados[i]["lon"]),
                    (pdvs_ordenados[i + 1]["lat"], pdvs_ordenados[i + 1]["lon"])
                )
                for i in range(len(pdvs_ordenados) - 1)
            )
            tempo_transito = (total_km / v_kmh) * 60
            tempo_servico = len(pdvs_ordenados) * service_min
            tempo_teorico = tempo_transito + tempo_servico

            # Diagnóstico (para auditoria)
            if tempo_teorico > workday_min:
                logger.warning(
                    f"⏱️ [Pré-OSRM] Sub {sub_id+1} do cluster {cluster.cluster_id} "
                    f"atingiu {tempo_teorico:.1f} min (> limite {workday_min})."
                )
            with open(diagnostic_log, "a", encoding="utf-8") as f:
                f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S},{cluster.cluster_id},{sub_id+1},{tempo_teorico:.1f},{total_km:.1f},{len(pdvs_ordenados)}\n")

            tempos_sub.append(tempo_teorico)
            dist_sub.append(total_km)
            pdvs_count.append(len(pdvs_ordenados))

            # Violação baseada em tempo e distância
            if tempo_teorico > workday_min or total_km > route_km_max:
                violacoes.append(True)

            subclusters.append({
                "subcluster_id": sub_id + 1,
                "n_pdvs": len(pdvs_sub),
                "tempo_total_min": tempo_teorico,
                "dist_total_km": total_km,
                "pdvs": pdvs_ordenados,
                "rota_coord": []
            })

        convergiu = not any(violacoes)
        if not convergiu:
            k = min(len(pdvs_cluster), k + max(1, int(k * 0.3)))
            logger.info(f"🔁 Reavaliando cluster {cluster.cluster_id} → Novo K={k}")
            continue

        # Pós-processamento
        subclusters, _ = consolidar_unitarios(subclusters, cluster, workday_min, route_km_max, service_min, v_kmh)
        subclusters = merge_small_routes(subclusters, min_pdvs=5)

        mean_tempo = np.mean(tempos_sub or [0])
        std_tempo = np.std(tempos_sub or [0])
        mean_pdvs = np.mean(pdvs_count or [0])
        max_tempo = max(tempos_sub or [0])
        max_dist = max(dist_sub or [0])

        logger.success(
            f"🛑 Convergência → Cluster {cluster.cluster_id} | K={k} | "
            f"Rotas={len(subclusters)} | Média={mean_tempo:.1f}±{std_tempo:.1f} min | "
            f"PDVs médios={mean_pdvs:.1f}"
        )
        break

    return {
        "cluster_id": cluster.cluster_id,
        "k_final": k,
        "total_pdvs": len(pdvs_cluster),
        "max_tempo": round(max_tempo, 1),
        "max_dist": round(max_dist, 1),
        "mean_tempo": round(mean_tempo, 1),
        "std_tempo": round(std_tempo, 1),
        "mean_pdvs": round(mean_pdvs, 1),
        "subclusters": subclusters,
        "violacoes": violacoes
    }


# ============================================================
# 🔹 Pipeline geral de subclusterização
# ============================================================
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
    resultados, violacoes_totais = [], []
    logger.info("🚀 Iniciando subclusterização adaptativa com convergência teórica...")

    for cluster in clusters:
        pdvs_cluster = [p for p in pdvs if p.cluster_id == cluster.cluster_id]
        if not pdvs_cluster:
            continue

        logger.info(f"\n🧭 Cluster {cluster.cluster_id} → {len(pdvs_cluster)} PDVs")
        resultado = dividir_cluster_em_subclusters(
            cluster, pdvs_cluster, workday_min, route_km_max, service_min, v_kmh, alpha_path, aplicar_two_opt
        )
        resultados.append(resultado)
        violacoes_totais.extend(resultado["violacoes"])

    if violacoes_totais:
        logger.warning(f"\n⚠️ {len(violacoes_totais)} restrições não atendidas.")
    else:
        logger.success("✅ Todas as rotas atendem aos limites operacionais.")

    return resultados

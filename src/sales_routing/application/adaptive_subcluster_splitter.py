import math
import numpy as np
import os
from sklearn.cluster import KMeans
from typing import List, Dict, Any
from datetime import datetime
from loguru import logger
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData
from src.sales_routing.application.route_optimizer import RouteOptimizer


# ============================================================
# 🔹 Função auxiliar: distância Haversine
# ============================================================
def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    """Distância Haversine em km entre dois pontos."""
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
    """Ordena PDVs pelo algoritmo Nearest Neighbor usando Haversine."""
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
# 🔹 Consolidação de subclusters unitários
# ============================================================
def consolidar_unitarios(subclusters, cluster, workday_min, route_km_max, service_min, v_kmh):
    """Agrupa subclusters com apenas 1 PDV ao mais próximo geograficamente."""
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

        # Ordena por distância do PDV ao centroide do subcluster de destino
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
                logger.debug(
                    f"   ↳ Sub {sub_unit['subcluster_id']} (1 PDV) agregado ao Sub {destino['subcluster_id']} "
                    f"({dist_extra:.1f} km)"
                )
                break

    if fundidos == 0:
        logger.info("ℹ️ Nenhum subcluster unitário pôde ser consolidado.")
        return subclusters, 0

    # ============================================================
    # 🔁 Reordena e recalcula rotas dos subclusters fundidos
    # ============================================================
    logger.info("🔄 Recalculando rotas após fusão de subclusters...")
    for sub in subclusters:
        centro = {"lat": cluster.centro_lat, "lon": cluster.centro_lon}
        pdvs_reordenados = nearest_neighbor_sequence(centro, sub["pdvs"])

        total_km = sum(
            haversine_km(
                (pdvs_reordenados[i]["lat"], pdvs_reordenados[i]["lon"]),
                (pdvs_reordenados[i + 1]["lat"], pdvs_reordenados[i + 1]["lon"])
            )
            for i in range(len(pdvs_reordenados) - 1)
        )
        tempo_min = (total_km / v_kmh) * 60 + len(pdvs_reordenados) * service_min

        sub["pdvs"] = pdvs_reordenados
        sub["tempo_total_min"] = tempo_min
        sub["dist_total_km"] = total_km

    logger.success(f"✅ {fundidos} subclusters unitários consolidados e rotas recalculadas.")
    return subclusters, fundidos


# ============================================================
# 🔹 Divisão adaptativa de clusters com NN + KMeans + consolidação
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
    """
    Divide adaptativamente um cluster em subclusters até que TODOS os subclusters
    respeitem tempo e distância máximos.
    Após convergência, consolida subclusters com 1 PDV ao mais próximo geograficamente
    e recalcula as rotas dos grupos fundidos.
    """

    k = 1
    convergiu = False
    resultados_iter = []
    violacoes = []

    log_path = "/tmp/cluster_split_debug.csv"
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    if not os.path.exists(log_path):
        with open(log_path, "w", encoding="utf-8") as f:
            f.write("timestamp,cluster_id,k,max_tempo,max_dist,convergiu\n")

    while not convergiu:
        coords = np.array([[p.lat, p.lon] for p in pdvs_cluster])
        kmeans = KMeans(n_clusters=k, random_state=42, n_init="auto").fit(coords)
        labels = kmeans.labels_

        subclusters = []
        tempos_sub, dist_sub = [], []
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
            tempo_min = (total_km / v_kmh) * 60 + len(pdvs_ordenados) * service_min

            tempos_sub.append(tempo_min)
            dist_sub.append(total_km)

            if tempo_min > workday_min:
                violacoes.append({
                    "cluster_id": cluster.cluster_id,
                    "subcluster_id": sub_id + 1,
                    "tipo": "tempo",
                    "valor": tempo_min,
                    "limite": workday_min
                })
            if total_km > route_km_max:
                violacoes.append({
                    "cluster_id": cluster.cluster_id,
                    "subcluster_id": sub_id + 1,
                    "tipo": "distancia",
                    "valor": total_km,
                    "limite": route_km_max
                })

            logger.debug(
                f"📍 Cluster {cluster.cluster_id} / Sub {sub_id + 1} → "
                f"{len(pdvs_sub)} PDVs | {total_km:.1f} km / {tempo_min:.1f} min (NN)"
            )

            subclusters.append({
                "subcluster_id": sub_id + 1,
                "n_pdvs": len(pdvs_sub),
                "tempo_total_min": tempo_min,
                "dist_total_km": total_km,
                "pdvs": pdvs_ordenados,
                "rota_coord": []
            })

        max_tempo = max(tempos_sub) if tempos_sub else 0
        max_dist = max(dist_sub) if dist_sub else 0
        convergiu = len(violacoes) == 0

        resultados_iter.append((k, max_tempo, max_dist, convergiu))
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{datetime.now():%Y-%m-%d %H:%M:%S},{cluster.cluster_id},{k},{max_tempo:.1f},{max_dist:.1f},{convergiu}\n")

        if convergiu:
            logger.success(
                f"🛑 Convergência atingida: todos os subclusters de {cluster.cluster_id} respeitam limites "
                f"(K={k}, MáxTempo={max_tempo:.1f} min, MáxDist={max_dist:.1f} km)"
            )
            subclusters, fundidos = consolidar_unitarios(subclusters, cluster, workday_min, route_km_max, service_min, v_kmh)
            if fundidos:
                logger.info(f"🏗️ Cluster {cluster.cluster_id}: {fundidos} unitários fundidos → {len(subclusters)} subclusters finais.")
            break

        if k >= len(pdvs_cluster):
            logger.warning(
                f"⚠️ Cluster {cluster.cluster_id}: limite de PDVs atingido ({len(pdvs_cluster)}). "
                "Forçando parada sem convergência."
            )
            break

        logger.debug(f"🔁 Iteração K={k}: MáxTempo={max_tempo:.1f} | MáxDist={max_dist:.1f} | Convergiu={convergiu}")
        k += 1

    logger.info(
        f"🏁 Cluster {cluster.cluster_id}: K_final={k}, MáxTempo={max_tempo:.1f} min, MáxDist={max_dist:.1f} km, Subclusters={len(subclusters)}"
    )

    return {
        "cluster_id": cluster.cluster_id,
        "k_final": k,
        "total_pdvs": len(pdvs_cluster),
        "max_tempo": round(max_tempo, 1),
        "max_dist": round(max_dist, 1),
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
    """Executa subclusterização adaptativa com auditoria final."""
    resultados = []
    violacoes_totais = []

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

    # Auditoria final
    if violacoes_totais:
        logger.warning("\n⚠️ Restrições não atendidas:")
        for v in violacoes_totais:
            logger.warning(
                f"   - Cluster {v['cluster_id']} / Sub {v['subcluster_id']} → "
                f"{v['tipo'].upper()} = {v['valor']:.1f} (limite {v['limite']})"
            )
    else:
        logger.success("✅ Todas as rotas atendem aos limites operacionais.")

    return resultados

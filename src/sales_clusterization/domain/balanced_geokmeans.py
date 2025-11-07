# ============================================================
# 📦 src/sales_clusterization/domain/balanced_geokmeans.py
# ============================================================

import numpy as np
from sklearn.cluster import KMeans
from loguru import logger
from typing import List, Tuple
from math import radians, sin, cos, sqrt, atan2


# ============================================================
# 🧮 Função auxiliar — Haversine
# ============================================================
def _haversine_km(coord1: Tuple[float, float], coord2: Tuple[float, float]) -> float:
    R = 6371.0
    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * atan2(sqrt(a), sqrt(1 - a))
    return R * c


# ============================================================
# ⚙️ Função principal
# ============================================================
def balanced_geokmeans(
    pdvs: List,
    k: int,
    max_pdv_cluster: int = None,
    max_iter: int = 20,
    tolerancia: int = 2,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Executa um KMeans geográfico e redistribui PDVs para manter tamanhos equilibrados,
    preservando coerência espacial.
    """

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    n_pdvs = len(coords)
    if not max_pdv_cluster:
        max_pdv_cluster = int(np.ceil(n_pdvs / k))

    logger.debug(f"📊 Execução inicial do GeoKMeans: K={k}, iterações={max_iter}")

    # ============================================================
    # 1️⃣ Execução inicial do KMeans
    # ============================================================
    kmeans = KMeans(n_clusters=k, n_init=20, random_state=42)
    labels = kmeans.fit_predict(coords)
    centers = kmeans.cluster_centers_

    # ============================================================
    # 2️⃣ Rebalanceamento iterativo por cardinalidade
    # ============================================================
    for it in range(max_iter):
        unique, counts = np.unique(labels, return_counts=True)
        excesso = {u: c - max_pdv_cluster for u, c in zip(unique, counts) if c > max_pdv_cluster + tolerancia}
        faltando = {u: max_pdv_cluster - c for u, c in zip(unique, counts) if c < max_pdv_cluster - tolerancia}

        logger.debug(f"🔁 Iteração {it+1}: excesso={excesso} | faltando={faltando}")

        if not excesso and not faltando:
            logger.success(f"⚖️ Rebalanceamento concluído após {it+1} iterações.")
            break

        for cid_excesso, qtd_excesso in excesso.items():
            idx_excesso = np.where(labels == cid_excesso)[0]
            if len(idx_excesso) == 0:
                continue

            # Distâncias entre pontos em excesso e centróides de outros clusters
            dist_to_centers = np.array([
                [_haversine_km(coords[i], centers[cid]) for cid in range(k)]
                for i in idx_excesso
            ])

            # Ordena PDVs mais distantes do seu centro atual (menos centrais)
            ordem = np.argsort(-dist_to_centers[:, cid_excesso])

            for i_local in ordem[:qtd_excesso]:
                destinos_validos = [cid for cid, f in faltando.items() if f > 0]
                if not destinos_validos:
                    break
                dist_ao_destino = {cid: dist_to_centers[i_local, cid] for cid in destinos_validos}
                destino = min(dist_ao_destino, key=dist_ao_destino.get)
                labels[idx_excesso[i_local]] = destino
                faltando[destino] -= 1

        # Atualiza centróides
        for cid in range(k):
            cluster_pts = coords[labels == cid]
            if len(cluster_pts) > 0:
                centers[cid] = cluster_pts.mean(axis=0)

    else:
        logger.warning(f"⚠️ Rebalanceamento não convergiu em {max_iter} iterações.")

    logger.success(f"✅ GeoKMeans balanceado concluído | {k} clusters formados.")
    return labels, centers

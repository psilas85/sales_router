# ============================================================
# 📦 src/sales_clusterization/domain/radial_balanced_clustering.py
# ============================================================

import numpy as np
from loguru import logger
from typing import List, Tuple
from math import radians, sin, cos, sqrt, atan2
from random import sample


# ============================================================
# ⚙️ Haversine (distância geográfica em km)
# ============================================================
def _haversine_km(coord1, coord2):
    R = 6371.0
    lat1, lon1 = radians(coord1[0]), radians(coord1[1])
    lat2, lon2 = radians(coord2[0]), radians(coord2[1])
    dlat, dlon = lat2 - lat1, lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


# ============================================================
# 🎯 Seleção inicial de centros (farthest-point sampling)
# ============================================================
def _inicializar_centros(coords: np.ndarray, k: int) -> np.ndarray:
    centros = [coords[np.random.choice(len(coords))]]
    while len(centros) < k:
        dist_min = np.min(
            np.array([[ _haversine_km(c, x) for c in centros] for x in coords]),
            axis=1
        )
        idx = np.argmax(dist_min)
        centros.append(coords[idx])
    return np.array(centros)


# ============================================================
# 🌐 Algoritmo principal — Radial Balanced Clustering
# ============================================================
def radial_balanced_clustering(
    pdvs: List,
    k: int,
    max_pdv_cluster: int,
    max_iter: int = 15,
    tolerancia: float = 0.001,
) -> Tuple[np.ndarray, np.ndarray]:
    """
    Clusterização radial equilibrada:
    - Divide o universo em k centros geométricos.
    - Atribui PDVs de forma radial até o limite máximo por cluster.
    - Ajusta centros para as áreas mais densas.
    """

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    n = len(coords)
    logger.info(f"🌎 Iniciando clusterização radial: {n} PDVs | K={k} | max={max_pdv_cluster}/cluster")

    # ================================
    # 1️⃣ Inicializa centros
    # ================================
    centros = _inicializar_centros(coords, k)
    labels = np.full(n, -1)
    logger.debug(f"🎯 Centros iniciais definidos por farthest-point sampling: {centros.shape[0]} pontos")

    # ================================
    # 2️⃣ Atribuição radial balanceada
    # ================================
    for it in range(max_iter):
        logger.debug(f"🔁 Iteração {it + 1}")

        # Reset das contagens
        cluster_sizes = np.zeros(k, dtype=int)
        labels[:] = -1

        # Calcula distâncias para todos os centros
        dist_matrix = np.zeros((n, k))
        for i in range(n):
            for j in range(k):
                dist_matrix[i, j] = _haversine_km(coords[i], centros[j])

        # Atribui cada ponto ao centro mais próximo disponível
        for i in np.argsort(np.min(dist_matrix, axis=1)):
            distancias = dist_matrix[i]
            destinos_ordenados = np.argsort(distancias)
            for dest in destinos_ordenados:
                if cluster_sizes[dest] < max_pdv_cluster:
                    labels[i] = dest
                    cluster_sizes[dest] += 1
                    break

        # ================================
        # 3️⃣ Recalcula centros (ajuste denso)
        # ================================
        novos_centros = np.zeros_like(centros)
        for j in range(k):
            cluster_pts = coords[labels == j]
            if len(cluster_pts) == 0:
                novos_centros[j] = centros[j]
                continue

            # Densidade local — medoide aproximado ponderado por distância média
            dist_matrix = np.array([[ _haversine_km(a, b) for b in cluster_pts] for a in cluster_pts])
            densidade = 1 / (np.mean(dist_matrix, axis=1) + 1e-6)
            densidade /= densidade.sum()
            novos_centros[j] = np.sum(cluster_pts * densidade[:, None], axis=0)

        # ================================
        # 4️⃣ Verifica convergência
        # ================================
        deslocamento = np.linalg.norm(novos_centros - centros, axis=1).mean()
        logger.debug(f"📏 Deslocamento médio dos centros: {deslocamento:.5f}")
        centros = novos_centros

        if deslocamento < tolerancia:
            logger.success(f"✅ Convergência alcançada após {it + 1} iterações.")
            break
    else:
        logger.warning(f"⚠️ Convergência não atingida após {max_iter} iterações.")

    # Diagnóstico
    cluster_info = {i: int(np.sum(labels == i)) for i in range(k)}
    logger.info(f"📊 Distribuição final dos clusters: {cluster_info}")

    return labels, centros

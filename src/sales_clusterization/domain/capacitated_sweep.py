# ============================================================
# 📦 src/sales_clusterization/domain/capacitated_sweep.py
# ============================================================

import numpy as np
from loguru import logger
from math import radians, sin, cos, atan2, sqrt
from collections import defaultdict

# ============================================================
# 📍 Funções auxiliares
# ============================================================

def _haversine_km(lat1, lon1, lat2, lon2):
    """Calcula distância Haversine (km)."""
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


def _angulo_polar(lat, lon, lat_ref, lon_ref):
    """Calcula o ângulo polar (em radianos) em relação a um ponto de referência."""
    dlon = radians(lon - lon_ref)
    y = sin(dlon) * cos(radians(lat))
    x = cos(radians(lat_ref)) * sin(radians(lat)) - sin(radians(lat_ref)) * cos(radians(lat)) * cos(dlon)
    ang = atan2(y, x)
    return ang if ang >= 0 else ang + 2 * np.pi


# ============================================================
# 🚀 Algoritmo principal
# ============================================================

def capacitated_sweep(pdvs, max_capacity=100, random_state=42):
    """
    Implementa o algoritmo Capacitated Sweep Clustering.
    Ideal para cenários lineares ou radiais (como PDVs ao longo de rodovias).
    Agrupa pontos em clusters contínuos com limite de capacidade.
    """
    np.random.seed(random_state)
    logger.info(f"🚀 Iniciando Capacitated Sweep Clustering | capacidade máxima={max_capacity}")

    # 1️⃣ Extrai coordenadas
    coords = np.array([[p.lat, p.lon] for p in pdvs])
    n = len(coords)
    if n == 0:
        raise ValueError("Nenhum PDV fornecido.")

    # 2️⃣ Define ponto de referência (centro geográfico)
    lat_ref, lon_ref = coords.mean(axis=0)
    logger.debug(f"📍 Centro de referência: ({lat_ref:.5f}, {lon_ref:.5f})")

    # 3️⃣ Calcula ângulo polar e distância radial
    angulos = np.array([_angulo_polar(lat, lon, lat_ref, lon_ref) for lat, lon in coords])
    distancias = np.array([_haversine_km(lat_ref, lon_ref, lat, lon) for lat, lon in coords])

    # 4️⃣ Ordena por ângulo e distância (varredura radial)
    ordem = np.lexsort((distancias, angulos))
    coords_ord = coords[ordem]
    pdvs_ord = [pdvs[i] for i in ordem]

    # 5️⃣ Cria clusters conforme capacidade
    clusters = []
    cluster_atual = []
    for p in pdvs_ord:
        cluster_atual.append(p)
        if len(cluster_atual) >= max_capacity:
            clusters.append(cluster_atual)
            cluster_atual = []
    if cluster_atual:
        clusters.append(cluster_atual)

    # 6️⃣ Calcula centróides
    centers = []
    for cluster in clusters:
        arr = np.array([[p.lat, p.lon] for p in cluster])
        centers.append(arr.mean(axis=0))
    centers = np.array(centers)

    # 7️⃣ Gera labels
    labels = np.zeros(n, dtype=int)
    idx_inicio = 0
    for cid, cluster in enumerate(clusters):
        for p in cluster:
            idx_global = pdvs.index(p)
            labels[idx_global] = cid
        idx_inicio += len(cluster)

    # 8️⃣ Métricas
    tamanhos = [len(c) for c in clusters]
    dist_medias = []
    for cid, cluster in enumerate(clusters):
        arr = np.array([[p.lat, p.lon] for p in cluster])
        centro = centers[cid]
        dists = [_haversine_km(p.lat, p.lon, centro[0], centro[1]) for p in cluster]
        dist_medias.append(np.mean(dists))
    media_intra = np.mean(dist_medias)

    # 9️⃣ Log final
    logger.success(f"✅ Capacitated Sweep concluído | K={len(clusters)} | média={np.mean(tamanhos):.1f} PDVs | dist_média_intra={media_intra:.2f} km")
    logger.debug(f"Tamanhos={tamanhos}")

    return labels, centers

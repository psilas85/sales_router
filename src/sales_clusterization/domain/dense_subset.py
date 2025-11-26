# ============================================================
# 📦 src/sales_clusterization/domain/dense_subset.py
# ============================================================

import numpy as np
from loguru import logger
from math import radians, sin, cos, sqrt, atan2
from typing import List
from src.sales_clusterization.domain.entities import PDV


# ------------------------------------------------------------
# 🌍 Distância Haversine
# ------------------------------------------------------------
def _haversine_km(p1, p2):
    R = 6371.0
    lat1, lon1 = p1
    lat2, lon2 = p2

    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)

    a = (
        sin(dlat / 2) ** 2
        + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    )
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


# ------------------------------------------------------------
# 🎯 Selecionar subset denso (compacto)
# ------------------------------------------------------------
def dense_subset(pdvs: List[PDV], capacidade: int = 160):
    """
    Seleciona os `capacidade` PDVs mais próximos entre si,
    usando medoide global + distâncias ordenadas.
    """

    total = len(pdvs)
    logger.info(f"🚀 DenseSubset: recebidos {total} PDVs | capacidade={capacidade}")

    if total <= capacidade:
        logger.warning("⚠️ Total de PDVs menor que capacidade — retornando todos.")
        return pdvs

    # Coordenadas básicas
    coords = np.array([[p.lat, p.lon] for p in pdvs])

    # 1) Calcular medoide global
    logger.info("📍 Calculando medoide global... (N², mas rápido para N<=5000)")

    sum_dist = np.zeros(total)
    for i, p in enumerate(coords):
        sum_dist[i] = np.sum([_haversine_km(p, q) for q in coords])

    medoide_idx = int(np.argmin(sum_dist))
    medoide = coords[medoide_idx]

    logger.info(f"📌 Medoide localizado no índice={medoide_idx}")

    # 2) Distância até medoide
    dist_med = np.array([_haversine_km(medoide, p) for p in coords])

    # 3) Ordenar
    sorted_idx = np.argsort(dist_med)

    # 4) Seleção final
    selecionados_idx = sorted_idx[:capacidade]
    selecionados = [pdvs[i] for i in selecionados_idx]

    raio_max = float(dist_med[selecionados_idx[-1]])

    logger.success(
        f"🏆 DenseSubset concluído | selecionados={len(selecionados)} | raio_max={raio_max:.2f} km"
    )

    return selecionados

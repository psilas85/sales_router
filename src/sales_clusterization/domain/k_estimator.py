#sales_router/src/sales_clusterization/domain/k_estimator.py

# ============================================================
# 📦 src/sales_clusterization/domain/k_estimator.py  (CORRIGIDO)
# ============================================================

import numpy as np
import math
from loguru import logger
from typing import List, Tuple, Dict


# ============================================================
# 🌍 Distância Haversine utilitária
# ============================================================
def _haversine_km(p1, p2):
    from math import radians, sin, cos, sqrt, atan2
    R = 6371.0
    lat1, lon1 = p1
    lat2, lon2 = p2
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


# ============================================================
# 💡 Cálculo principal do K inicial (macroclusters)
# ============================================================
def estimar_k_inicial(
    pdvs: List,
    workday_min: float,
    route_km_max: float,
    service_min: float,
    v_kmh: float,
    dias_uteis: int,
    freq: int,
    max_pdv_cluster: int,
    alpha_path: float,
) -> Tuple[int, Dict]:
    """
    Cálculo correto do K inicial com base em:
    - número total de PDVs
    - capacidade por cluster ajustada pela frequência
    """

    n_pdvs = len(pdvs)
    if n_pdvs == 0:
        raise ValueError("Nenhum PDV informado para cálculo do K inicial.")

    # ============================================================
    # 🚀 CORREÇÃO CRÍTICA:
    # Capacidade real por cluster = max_pdv_cluster * freq
    # K inicial = ceil(n_pdvs / capacidade)
    # ============================================================
    capacidade_real = max_pdv_cluster * max(freq, 1)

    k_estimado = max(1, math.ceil(n_pdvs / capacidade_real))

    # ============================================================
    # 📊 Diagnóstico detalhado
    # ============================================================
    diag = {
        "pdvs_totais": n_pdvs,
        "max_pdv_cluster": max_pdv_cluster,
        "freq": freq,
        "capacidade_real_por_cluster": capacidade_real,
        "dias_uteis": dias_uteis,
        "workday_min": workday_min,
        "route_km_max": route_km_max,
        "service_min": service_min,
        "v_kmh": v_kmh,
        "alpha_path": alpha_path,
        "k_inicial": k_estimado,
        "criterio": "ceil(n_pdvs / capacidade_real)",
    }

    logger.info(
        f"🧮 K inicial estimado = {k_estimado} "
        f"(n_pdvs={n_pdvs}, capacidade={capacidade_real} PDVs/cluster)"
    )

    return k_estimado, diag


# ============================================================
# ⚙️ Função auxiliar opcional (diagnóstico geográfico)
# ============================================================
def estimar_k_por_raio(pdvs: List, raio_km: float = 5.0) -> int:
    """
    Estima K baseado em densidade para propósitos diagnósticos.
    Não interfere no cálculo principal.
    """
    if len(pdvs) < 2:
        return 1

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    lats, lons = coords[:, 0], coords[:, 1]
    lat_med, lon_med = np.mean(lats), np.mean(lons)

    dists = np.array(
        [_haversine_km((lat_med, lon_med), (lat, lon)) for lat, lon in zip(lats, lons)]
    )
    raio_global = np.percentile(dists, 95)
    area_coberta = np.pi * (raio_km**2)
    densidade = len(pdvs) / (np.pi * raio_global**2)

    k_estimado = max(1, int(math.ceil(len(pdvs) / (densidade * area_coberta))))

    logger.debug(
        f"🌐 Estimativa geográfica → densidade={densidade:.2f} pts/km² | "
        f"raio_p95={raio_global:.1f} km | K≈{k_estimado}"
    )

    return k_estimado

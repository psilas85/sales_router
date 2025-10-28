# ============================================================
# 📦 src/sales_clusterization/domain/haversine_utils.py
# ============================================================

import math

def haversine(coord1, coord2):
    """
    Calcula a distância entre dois pontos (lat, lon) em quilômetros.
    """
    R = 6371  # raio médio da Terra em km
    lat1, lon1 = coord1
    lat2, lon2 = coord2

    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)

    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

# sales_clusterization/domain/k_estimator.py

# sales_clusterization/domain/k_estimator.py

import math, random
from typing import List, Tuple
from .entities import PDV


# ============================================================
# 🔹 Distância haversine entre dois pontos (km)
# ============================================================
def _haversine_km(a: Tuple[float, float], b: Tuple[float, float]) -> float:
    R = 6371.0
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)
    dlat, dlon = lat2 - lat1, lon2 - lon1
    h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    return 2 * R * math.asin(math.sqrt(h))


# ============================================================
# 🔹 Calcula mediana da distância ao vizinho mais próximo
# ============================================================
def mediana_vizinho_mais_proximo(pdvs: List[PDV], amostra: int = 2000) -> float:
    """
    Retorna a mediana das distâncias entre cada PDV e seu vizinho mais próximo.
    - Amostra até 2000 PDVs.
    - Retorna 0 se houver menos de 2 PDVs válidos (evita erro no min()).
    """
    if not pdvs or len(pdvs) < 2:
        return 0.0

    pts = [(p.lat, p.lon) for p in pdvs if p.lat and p.lon]
    if len(pts) < 2:
        return 0.0

    sample = random.sample(pts, min(amostra, len(pts)))
    dists = []

    for s in sample:
        vizinhos = [_haversine_km(s, t) for t in pts if t != s]
        if vizinhos:  # evita sequência vazia no min()
            dists.append(min(vizinhos))

    if not dists:
        return 0.0

    dists.sort()
    mid = len(dists) // 2
    return dists[mid]


# ============================================================
# 🔹 Estima número inicial de clusters (K0)
# ============================================================
def estimar_k_inicial(
    pdvs: List[PDV],
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float,
    dias_uteis: int,
    freq: int,
    alpha_path: float = 1.4,
) -> Tuple[int, dict]:
    N = len(pdvs)
    d_med = mediana_vizinho_mais_proximo(pdvs)

    # tempo médio de deslocamento entre PDVs (min)
    t_mov = (d_med / max(v_kmh, 1e-6)) * 60.0
    # tempo total por PDV (serviço + deslocamento médio)
    t_por_pdv = service_min + t_mov

    # total de tempo necessário (min) para atender todos PDVs na frequência desejada
    t_total_necessario = N * freq * t_por_pdv

    # total de tempo disponível (min) por vendedor no ciclo
    t_total_vendedor = dias_uteis * workday_min

    # quantidade de vendedores necessária
    vendedores = max(1, math.ceil(t_total_necessario / max(t_total_vendedor, 1)))

    # cálculo auxiliar para estimativa de rotas
    pdvs_por_rota_time = max(1, int(workday_min // max(t_por_pdv, 1)))
    pdvs_por_rota_dist = max(1, int(route_km_max // max(alpha_path * d_med, 1e-6)))
    cap_pdv_por_rota = max(1, min(pdvs_por_rota_time, pdvs_por_rota_dist))
    rotas_por_ciclo = math.ceil(N / cap_pdv_por_rota)

    k0 = vendedores

    return k0, {
        "N": N,
        "d_med_km": d_med,
        "t_mov_min": t_mov,
        "t_por_pdv_min": t_por_pdv,
        "t_total_necessario_min": t_total_necessario,
        "t_total_vendedor_min": t_total_vendedor,
        "pdvs_por_rota_time": pdvs_por_rota_time,
        "pdvs_por_rota_dist": pdvs_por_rota_dist,
        "cap_pdv_por_rota": cap_pdv_por_rota,
        "rotas_por_ciclo": rotas_por_ciclo,
        "vendedores": vendedores,
    }

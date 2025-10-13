#sales_clusterization/domain/validators.py

from typing import List
from .entities import Setor

def checar_raio(setores: List[Setor], route_km_max: float) -> bool:
    # regra de bolso: raio_p95 * 3 <= route_km_max (aproximação para “caber” em subrotas)
    return all(s.raio_p95_km * 3.0 <= route_km_max for s in setores)

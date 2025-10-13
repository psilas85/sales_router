# src/sales_routing/domain/entities/cluster_data_entity.py

from dataclasses import dataclass
from typing import Dict, Optional


@dataclass
class ClusterData:
    run_id: int
    cluster_id: int
    cluster_label: int
    centro_lat: float
    centro_lon: float
    n_pdvs: int
    metrics: Dict[str, float]


@dataclass
class PDVData:
    run_id: int
    cluster_id: int
    pdv_id: int
    lat: float
    lon: float
    cidade: str
    uf: str
    nome: Optional[str] = None

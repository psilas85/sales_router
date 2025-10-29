#sales_clusterization/domain/entities.py

from dataclasses import dataclass
from typing import Optional


@dataclass
class PDV:
    """Representa um ponto de venda (cliente)."""
    id: int
    cnpj: str
    nome: Optional[str]
    cidade: Optional[str]
    uf: Optional[str]
    lat: float
    lon: float


@dataclass
class Setor:
    """Representa um setor (cluster geográfico) de PDVs."""
    cluster_label: int
    centro_lat: float
    centro_lon: float
    n_pdvs: int
    raio_med_km: float
    raio_p95_km: float

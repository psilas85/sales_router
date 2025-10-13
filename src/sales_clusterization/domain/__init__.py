from dataclasses import dataclass
from typing import Optional

@dataclass(frozen=True)
class PDV:
    id: int
    cnpj: str
    nome: str
    bairro: str
    cidade: str
    uf: str
    lat: float
    lon: float

@dataclass
class Setor:
    cluster_label: int
    centro_lat: float
    centro_lon: float
    n_pdvs: int
    raio_med_km: float
    raio_p95_km: float

# ==========================================================
# 📦 src/sales_clusterization/domain/entities.py
# ==========================================================

from dataclasses import dataclass, field
from typing import Optional, Dict, List


# ==========================================================
# 🏪 Entidade PDV
# ==========================================================
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
    cluster_label: Optional[int] = None  # Cluster principal (macro)
    subcluster_seq: Optional[int] = None  # 🔹 Subcluster dentro do cluster principal


# ==========================================================
# 🗺️ Entidade Setor (cluster geográfico)
# ==========================================================
@dataclass
class Setor:
    """
    Representa um setor (cluster geográfico) de PDVs.
    - Compatível com pipelines de PDV e Marketplace.
    - Inclui campos opcionais pdvs/coords para avaliação operacional.
    """
    cluster_label: int
    centro_lat: float
    centro_lon: float
    n_pdvs: int
    raio_med_km: float
    raio_p95_km: float
    metrics: Dict[str, float] = field(default_factory=dict)

    # 🔹 Campos opcionais para integração com o refinamento operacional
    pdvs: Optional[List[PDV]] = None
    coords: Optional[List[tuple]] = None

    # 🔹 Campo hierárquico (para subdivisão interna)
    subclusters: List[Dict[str, float]] = field(default_factory=list)

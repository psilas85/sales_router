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
    # Janela de atendimento em minutos desde meia-noite (ex.: 480 = 08:00).
    # Quando ambos None → respeitar janela default do tenant ou ignorar.
    janela_atendimento_inicio: Optional[int] = None
    janela_atendimento_fim: Optional[int] = None
    # Sobrescreve service_min global para este PDV específico.
    tempo_atendimento_min: Optional[float] = None
    # Marca PDV estratégico (loja-âncora, grande conta) — vira disjunction
    # quase-obrigatória no solver CVRPTW.
    is_estrategico: Optional[bool] = None

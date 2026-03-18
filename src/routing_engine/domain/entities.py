#sales_router/src/routing_engine/domain/entities.py

from dataclasses import dataclass, field
from typing import Optional, List, Dict


@dataclass
class PDVData:
    pdv_id: int
    cnpj: str
    nome_fantasia: Optional[str]
    logradouro: str
    numero: Optional[str]
    bairro: Optional[str]
    cidade: str
    uf: str
    cep: Optional[str]
    grupo_utilizado: str
    fonte_grupo: str  # "setor" ou "consultor"
    lat: float
    lon: float
    freq_visita: float = 1.0

    @property
    def endereco_completo(self) -> str:
        numero = (self.numero or "").strip() or "S/N"
        bairro = (self.bairro or "").strip()
        cep = (self.cep or "").strip()

        partes = [
            f"{self.logradouro}, {numero}",
            bairro if bairro else None,
            f"{self.cidade} - {self.uf}",
            cep if cep else None,
        ]
        return " | ".join([p for p in partes if p])


@dataclass
class RouteGroup:
    group_id: str
    group_type: str  # "setor" ou "consultor"
    centro_lat: float
    centro_lon: float
    n_pdvs: int
    pdvs: List[PDVData] = field(default_factory=list)


@dataclass
class RouteStop:
    pdv_id: int
    cnpj: str
    nome_fantasia: Optional[str]
    endereco_completo: str
    logradouro: str
    numero: Optional[str]
    bairro: Optional[str]
    cidade: str
    uf: str
    cep: Optional[str]
    grupo_utilizado: str
    fonte_grupo: str
    sequencia: int
    lat: float
    lon: float


@dataclass
class RouteResult:
    rota_id: str
    subcluster_id: int
    grupo_utilizado: str
    fonte_grupo: str
    centro_lat: float
    centro_lon: float
    n_pdvs: int
    dist_total_km: float
    tempo_total_min: float
    stops: List[RouteStop] = field(default_factory=list)
    rota_coord: List[Dict] = field(default_factory=list)
# src/pdv_preprocessing/entities/pdv_entity.py

from dataclasses import dataclass
from typing import Optional

@dataclass
class PDV:
    cnpj: str
    logradouro: str
    numero: str
    bairro: str
    cidade: str
    uf: str
    cep: str
    pdv_endereco_completo: Optional[str] = None
    pdv_lat: Optional[float] = None
    pdv_lon: Optional[float] = None
    status_geolocalizacao: Optional[str] = None
    tenant_id: Optional[str] = None

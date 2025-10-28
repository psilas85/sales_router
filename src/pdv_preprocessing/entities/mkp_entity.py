#sales_router/src/pdv_preprocessing/entities/mkp_entity.py

# ============================================================
# 📦 src/pdv_preprocessing/entities/mkp_entity.py
# ============================================================

from dataclasses import dataclass
from typing import Optional


@dataclass
class MKP:
    """
    Entidade base para registro de marketplace (agregado por CEP).
    Representa uma linha validada e georreferenciada.
    """

    cidade: str
    uf: str
    bairro: Optional[str]
    cep: str
    clientes_total: int
    clientes_target: Optional[int] = None

    # Campos adicionados após georreferenciamento
    lat: Optional[float] = None
    lon: Optional[float] = None
    status_geolocalizacao: Optional[str] = None
    motivo_invalidade_geo: Optional[str] = None

    # Metadados operacionais
    tenant_id: Optional[int] = None
    input_id: Optional[str] = None
    descricao: Optional[str] = None


    def __post_init__(self):
        self.cidade = self.cidade.strip().upper()
        self.uf = self.uf.strip().upper()
        self.bairro = str(self.bairro or "").strip().upper()
        self.cep = str(self.cep).strip().zfill(8) if self.cep else None

        if not self.cidade or not self.uf or not self.cep or self.clientes_total is None:
            raise ValueError("❌ Campos obrigatórios ausentes: cidade, uf, cep, clientes_total.")

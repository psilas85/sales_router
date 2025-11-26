#sales_router/src/pdv_preprocessing/entities/mkp_entity.py

# ============================================================
# 📦 src/pdv_preprocessing/entities/mkp_entity.py
# ============================================================

from dataclasses import dataclass
from typing import Optional


@dataclass
class MKP:
    cidade: str
    uf: str
    bairro: Optional[str]
    cep: str
    clientes_total: int
    clientes_target: Optional[int] = None

    lat: Optional[float] = None
    lon: Optional[float] = None
    status_geolocalizacao: Optional[str] = None
    motivo_invalidade_geo: Optional[str] = None

    tenant_id: Optional[int] = None
    input_id: Optional[str] = None
    descricao: Optional[str] = None


    def __post_init__(self):

        # -----------------------
        # Campos textuais
        # -----------------------
        self.cidade = (self.cidade or "").strip().upper()
        self.uf = (self.uf or "").strip().upper()
        self.bairro = (self.bairro or "").strip().upper()

        # -----------------------
        # CEP seguro
        # -----------------------
        if self.cep:
            cep_str = str(self.cep).replace("-", "").strip()
            self.cep = cep_str.zfill(8) if cep_str.isdigit() else None
        else:
            self.cep = None

        # -----------------------
        # Conversões numéricas
        # -----------------------
        try:
            self.clientes_total = int(self.clientes_total)
        except:
            self.clientes_total = 0

        if self.clientes_target is not None:
            try:
                self.clientes_target = int(self.clientes_target)
            except:
                self.clientes_target = None

        # -----------------------
        # Regras de obrigatórios
        # -----------------------
        if (
            not self.cidade
            or not self.uf
            or not self.cep
            or self.clientes_total is None
        ):
            raise ValueError(
                "❌ Campos obrigatórios ausentes: cidade, uf, cep, clientes_total."
            )

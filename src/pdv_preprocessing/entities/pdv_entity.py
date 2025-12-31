# sales_router/src/pdv_preprocessing/entities/pdv_entity.py

from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PDV:
    """
    Entidade PDV (Ponto de Venda)
    Representa um registro de cliente/endereço geolocalizado pertencente a um tenant.
    """

    # ============================================================
    # Identificação e endereço
    # ============================================================
    cnpj: str
    logradouro: str
    numero: str
    bairro: str
    cidade: str
    uf: str
    cep: str
    pdv_vendas: Optional[float] = None

    # ============================================================
    # Metadados do processamento
    # ============================================================
    input_id: Optional[str] = None
    descricao: Optional[str] = None

    # ============================================================
    # Endereço completo e cache (CHAVE CANÔNICA)
    # ============================================================
    pdv_endereco_completo: Optional[str] = None
    endereco_cache_key: Optional[str] = None   # 👈 NOVO (OBRIGATÓRIO)

    # ============================================================
    # Dados de localização e status
    # ============================================================
    pdv_lat: Optional[float] = None
    pdv_lon: Optional[float] = None
    status_geolocalizacao: Optional[str] = None

    # ============================================================
    # Dados administrativos
    # ============================================================
    tenant_id: Optional[int] = field(default=None)
    id: Optional[int] = field(default=None)
    atualizado_em: Optional[str] = field(default=None)

    # ============================================================
    # Pós-processamento
    # ============================================================
    def __post_init__(self):
        """Normaliza e valida tipos logo após a criação da instância."""

        # Normaliza CNPJ e CEP
        self.cnpj = str(self.cnpj).strip()
        self.cep = str(self.cep).strip()

        # Normaliza cache key (string simples)
        if self.endereco_cache_key is not None:
            self.endereco_cache_key = str(self.endereco_cache_key).strip()

        # Converte tenant_id para int se vier como string
        if self.tenant_id is not None:
            try:
                self.tenant_id = int(self.tenant_id)
            except ValueError:
                raise ValueError(f"❌ tenant_id inválido: {self.tenant_id}")

        # Converte coordenadas se vierem como strings
        if isinstance(self.pdv_lat, str) and self.pdv_lat.strip():
            self.pdv_lat = float(self.pdv_lat)

        if isinstance(self.pdv_lon, str) and self.pdv_lon.strip():
            self.pdv_lon = float(self.pdv_lon)

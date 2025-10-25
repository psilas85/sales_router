from dataclasses import dataclass, field
from typing import Optional


@dataclass
class PDV:
    """
    Entidade PDV (Ponto de Venda)
    Representa um registro de cliente/endereço geolocalizado pertencente a um tenant.
    """

    # Identificação e vinculação
    cnpj: str
    logradouro: str
    numero: str
    bairro: str
    cidade: str
    uf: str
    cep: str

    # Metadados do processamento
    input_id: Optional[str] = None
    descricao: Optional[str] = None

    # Dados de localização e status
    pdv_endereco_completo: Optional[str] = None
    pdv_lat: Optional[float] = None
    pdv_lon: Optional[float] = None
    status_geolocalizacao: Optional[str] = None

    # Dados administrativos
    tenant_id: Optional[int] = field(default=None)
    id: Optional[int] = field(default=None)
    atualizado_em: Optional[str] = field(default=None)

    def __post_init__(self):
        """Normaliza e valida tipos logo após a criação da instância."""

        # Normaliza CNPJ e CEP
        self.cnpj = str(self.cnpj).strip()
        self.cep = str(self.cep).strip()

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

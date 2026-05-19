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
    # Identificação opcional (vinda do XLSX) — útil pra UI/relatórios.
    # ============================================================
    razao_social: Optional[str] = None
    nome_fantasia: Optional[str] = None

    # ============================================================
    # Roteirização com janelas (CVRPTW) — todos opcionais.
    # Quando ausentes, o solver cai nos fallbacks configurados no tenant
    # ou no horário de operação. Ver sales_routing/application/
    # cvrptw_subcluster_splitter.py para a semântica completa.
    # ============================================================
    janela_atendimento_inicio: Optional[int] = None   # min desde 0h
    janela_atendimento_fim: Optional[int] = None      # min desde 0h
    tempo_atendimento_min: Optional[float] = None     # sobrescreve service_min
    is_estrategico: Optional[bool] = None             # disjunction quase-obrigatória

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

        # ============================================================
        # Sanitiza NaN/pd.NA → None nos campos opcionais de roteirização.
        # Necessário porque o pandas converte None→NaN ao tipar colunas
        # float64, e o PG aceita NaN como valor válido (≠ NULL), o que
        # quebra o solver CVRPTW depois.
        # ============================================================
        import math
        for _attr in (
            "janela_atendimento_inicio",
            "janela_atendimento_fim",
            "tempo_atendimento_min",
            "is_estrategico",
        ):
            _val = getattr(self, _attr, None)
            if _val is None:
                continue
            try:
                if isinstance(_val, float) and math.isnan(_val):
                    setattr(self, _attr, None)
                    continue
            except (TypeError, ValueError):
                pass
            # Cobre pd.NA / NaT sem importar pandas aqui
            if str(_val).strip().lower() in {"nan", "<na>", "nat", "none", "null", ""}:
                setattr(self, _attr, None)

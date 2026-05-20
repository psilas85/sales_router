# sales_router/src/cadastros/entities/pdv_entity.py

from dataclasses import dataclass
from datetime import datetime
from typing import Optional
from uuid import UUID


@dataclass
class CadastroPDV:
    """
    Fonte canônica de PDV (cliente) por tenant.
    Independente de carregamentos (input_id) — pode alimentar tanto a
    Simulação Inteligente quanto a Execução Operacional.
    """

    id: Optional[UUID]
    tenant_id: int
    ativo: bool

    # Identificação
    cnpj: str
    razao_social: Optional[str]
    nome_fantasia: Optional[str]

    # Endereço (obrigatórios)
    logradouro: str
    numero: str
    bairro: str
    cidade: str
    uf: str
    cep: str

    # Geolocalização — preenchida via geocoding_engine ou override manual
    pdv_lat: Optional[float]
    pdv_lon: Optional[float]
    status_geolocalizacao: Optional[str]

    # Comercial
    pdv_vendas: Optional[float]

    # CVRPTW (janelas e estratégico)
    janela_atendimento_inicio: Optional[int]
    janela_atendimento_fim: Optional[int]
    tempo_atendimento_min: Optional[float]
    is_estrategico: Optional[bool]

    # Procedência do registro: 'manual' | 'xlsx' | 'sim_inteligente'
    origem: str = "manual"

    # Marca o PDV para revisão (ex.: cidade fora da lista IBGE ou sem
    # geocodificação na importação em lote). PDV revisar continua ativo.
    revisao_pendente: bool = False

    criado_em: Optional[datetime] = None
    atualizado_em: Optional[datetime] = None

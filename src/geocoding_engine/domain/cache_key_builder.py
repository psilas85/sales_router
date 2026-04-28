#sales_router/src/geocoding_engine/domain/cache_key_builder.py

import re

from geocoding_engine.domain.address_normalizer import normalize_for_cache


def _normalize_street_prefix(logradouro: str) -> str:
    if not logradouro:
        return ""

    normalized = str(logradouro).strip().upper()
    normalized = re.sub(r"^AV\.?\s+", "AVENIDA ", normalized)
    normalized = re.sub(r"^R\.?\s+", "RUA ", normalized)
    normalized = re.sub(r"^ROD\.?\s+", "RODOVIA ", normalized)
    normalized = re.sub(r"^AL\.?\s+", "ALAMEDA ", normalized)
    normalized = re.sub(r"^EST\.?\s+", "ESTRADA ", normalized)
    normalized = re.sub(r"^TRAV\.?\s+", "TRAVESSA ", normalized)
    normalized = re.sub(r"^PCA\.?\s+", "PRACA ", normalized)
    normalized = re.sub(r"^PC\.?\s+", "PRACA ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def build_canonical_address(
    logradouro: str,
    numero: str,
    cidade: str,
    uf: str,
) -> str:
    logradouro = _normalize_street_prefix(logradouro)
    numero = str(numero or "").strip()
    cidade = str(cidade or "").strip().upper()
    uf = str(uf or "").strip().upper()

    endereco_base = f"{logradouro} {numero}, {cidade} - {uf}"
    return re.sub(r"\s+", " ", endereco_base.replace(" ,", ",")).strip()


def build_cache_key(
    logradouro: str,
    numero: str,
    cidade: str,
    uf: str,
) -> str:
    """
    🔥 Gera chave de cache PADRÃO e IMUTÁVEL

    Regras:
    - NÃO usa bairro
    - NÃO usa complemento
    - NÃO usa CEP
    - formato fixo
    """

    return normalize_for_cache(
        build_canonical_address(logradouro, numero, cidade, uf)
    )
#sales_router/src/geocoding_engine/domain/cache_key_builder.py

from geocoding_engine.domain.address_normalizer import normalize_for_cache


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

    logradouro = str(logradouro or "").strip()
    numero = str(numero or "").strip()
    cidade = str(cidade or "").strip()
    uf = str(uf or "").strip()

    endereco_base = f"{logradouro} {numero}, {cidade} - {uf}"
    endereco_base = endereco_base.replace(" ,", ",").strip()

    return normalize_for_cache(endereco_base)
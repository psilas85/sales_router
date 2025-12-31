# tests/pdv_preprocessing/domain/test_address_normalizer_real_case.py

from pdv_preprocessing.domain.address_normalizer import (
    normalize_base,
    normalize_for_geocoding,
    normalize_for_cache,
)


def test_real_pdv_address_normalization_exact_case():
    # === INPUT REAL (CSV) ===
    raw_address = (
        "ARNAUD GUEDES AMORIM 145, "
        "COELHO DA ROCHA, "
        "SAO JOAO DE MERITI - RJ, "
        "25550-570, Brasil"
    )

    # === BASE ===
    base = normalize_base(raw_address)

    assert base == (
        "ARNAUD GUEDES AMORIM 145, "
        "COELHO DA ROCHA, "
        "SAO JOAO DE MERITI - RJ, "
        "25550-570"
    )

    # === GEOCODING (HUMANO / LEGÍVEL) ===
    geocoding = normalize_for_geocoding(base)

    assert geocoding == (
        "Arnaud Guedes Amorim 145, "
        "Coelho Da Rocha, "
        "Sao Joao De Meriti - Rj, "
        "25550-570"
    )

    # === CACHE (CHAVE CANÔNICA) ===
    cache_key = normalize_for_cache(base)

    assert cache_key == (
        "ARNAUD GUEDES AMORIM 145, "
        "COELHO DA ROCHA, "
        "SAO JOAO DE MERITI - RJ, "
        "25550-570"
    )

    # === ASSERTS DEFENSIVOS (ANTI-BUG) ===
    assert len(cache_key) > 30
    assert "-" in cache_key
    assert "25550-570" in cache_key
    assert "ARNAUD" in cache_key
    assert "MERITI" in cache_key

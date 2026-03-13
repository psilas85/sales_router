#sales_router/src/geocoding_engine/domain/address_normalizer.py

import re
import unicodedata

from pdv_preprocessing.utils.endereco_normalizer import (
    corrigir_truncados,
    expandir_abreviacoes,
)

# ============================================================
# 🔤 Utils internos
# ============================================================

def _remover_acentos(s: str) -> str:
    return (
        unicodedata.normalize("NFKD", s)
        .encode("ascii", "ignore")
        .decode("ascii")
    )


def _limpeza_basica(s: str) -> str:
    s = s.strip()
    s = re.sub(r"\s{2,}", " ", s)
    s = re.sub(r",\s*,", ", ", s)
    s = re.sub(r"^\s*,|\s*,\s*$", "", s)
    return s


# ============================================================
# 🧱 NORMALIZAÇÃO BASE (OBRIGATÓRIA)
# ============================================================
# - NÃO remove acentos
# - NÃO muda caixa
# - NÃO “fica esperta”
# - apenas padroniza forma
# ============================================================

def normalize_base(endereco: str) -> str:
    if not endereco:
        return ""

    s = endereco.strip()

    # normaliza espaços e vírgulas
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*,\s*", ", ", s)

    # remove "Brasil" no final
    s = re.sub(r",?\s*Brasil$", "", s, flags=re.I)

    return s.strip()


# ============================================================
# 🧭 1) PARA GEOCODIFICAÇÃO (Nominatim Local + Google)
# ============================================================
# REGRAS:
# - conservador
# - NÃO expandir AL / AV / R
# - remover lixo que quebra o geocoder
# ============================================================

def normalize_for_geocoding(endereco: str) -> str:
    if not endereco:
        return ""

    s = normalize_base(endereco)

    # correções humanas leves (podem alterar caixa)
    s = corrigir_truncados(s)

    # mantém legível para geocoder
    s = s.title()

    # expansões SEGURAS
    s = re.sub(r"\bSta\b", "Santa", s)
    s = re.sub(r"\bSto\b", "Santo", s)
    s = re.sub(r"\bS\b", "São", s)

    # ✅ REMOVE PREFIXO DE LOGRADOURO APENAS SE ISOLADO NO INÍCIO
    # Exemplos válidos:
    # "Alameda Paulista" → "Paulista"
    # "Al Alfredo Albuquerque" → "Alfredo Albuquerque" ❌ (NÃO remove)
    s = re.sub(
        r"^(?:Alameda|Avenida|Rua|Travessa|Rodovia|Estrada|Av\.?|R\.?)\s+",
        "",
        s,
        flags=re.IGNORECASE
    )

    # remove complementos finais que confundem geocoders
    s = re.sub(
        r"\b(Bloco|Bl|Loja|Lj|Sala|Sl|Apto|Apt|Cj|Conj)\b.*$",
        "",
        s,
        flags=re.IGNORECASE
    )

    return _limpeza_basica(s)



# ============================================================
# 🧠 2) PARA CACHE (CHAVE CANÔNICA)
# ============================================================
# REGRAS:
# - agressivo
# - determinístico
# - sempre gera a mesma chave
# ============================================================

def normalize_for_cache(endereco: str) -> str:
    if not endereco:
        return ""

    # base limpa, sem "Brasil"
    s = normalize_base(endereco)

    # cache precisa ser determinístico, NÃO esperto
    s = _remover_acentos(s)
    s = s.upper()

    # NÃO aplicar:
    # - corrigir_truncados
    # - expandir_abreviacoes
    # isso é só para geocoding, nunca para cache

    # remove lixo, mantém estrutura
    s = re.sub(r"[^A-Z0-9 ,\-]", "", s)

    # normaliza espaços e vírgulas
    s = _limpeza_basica(s)

    return s




# ============================================================
# 👁️ 3) DISPLAY / LOG
# ============================================================

def normalize_for_display(endereco: str) -> str:
    if not endereco:
        return ""

    return normalize_base(endereco)

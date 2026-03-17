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
# 🧱 NORMALIZAÇÃO BASE
# ============================================================

def normalize_base(endereco: str) -> str:
    if not endereco:
        return ""

    s = endereco.strip()

    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*,\s*", ", ", s)

    s = re.sub(r",?\s*Brasil$", "", s, flags=re.I)

    return s.strip()


# ============================================================
# 🧭 PARA GEOCODIFICAÇÃO (FIXADO)
# ============================================================

def normalize_for_geocoding(endereco: str) -> str:
    if not endereco:
        return ""

    original = endereco

    s = normalize_base(endereco)

    # correções leves
    s = corrigir_truncados(s)

    # padroniza casing
    s = s.title()

    # expansões SEGURAS
    s = re.sub(r"\bSta\b", "Santa", s)
    s = re.sub(r"\bSto\b", "Santo", s)
    s = re.sub(r"\bS\b", "São", s)

    # ❌ NÃO remover logradouro (isso só piora resultado)
    # REMOVIDO

    # ============================================================
    # ✅ FIX PRINCIPAL: NÃO destruir cidade
    # ============================================================

    # remove apenas o trecho do complemento, NÃO o resto da string
    s = re.sub(
        r"\b(Bloco|Bl|Loja|Lj|Sala|Sl|Apto|Apt|Cj|Conj)\b[^,]*",
        "",
        s,
        flags=re.IGNORECASE
    )

    s = _limpeza_basica(s)

    # ============================================================
    # 🔒 GARANTIA: cidade/UF nunca somem
    # ============================================================

    # tenta extrair cidade/UF do original se sumiram
    match = re.search(r",\s*([^,]+)\s*-\s*([A-Z]{2})$", original, re.IGNORECASE)

    if match:
        cidade = match.group(1).strip().title()
        uf = match.group(2).upper()

        if cidade.lower() not in s.lower():
            s = f"{s}, {cidade} - {uf}"

    return s


# ============================================================
# 🧠 CACHE
# ============================================================

def normalize_for_cache(endereco: str) -> str:
    if not endereco:
        return ""

    s = normalize_base(endereco)

    s = _remover_acentos(s)
    s = s.upper()

    s = re.sub(r"[^A-Z0-9 ,\-]", "", s)

    s = _limpeza_basica(s)

    return s


# ============================================================
# 👁️ DISPLAY
# ============================================================

def normalize_for_display(endereco: str) -> str:
    if not endereco:
        return ""

    return normalize_base(endereco)
# sales_router/src/geocoding_engine/domain/address_normalizer.py

import re
import unicodedata

from pdv_preprocessing.utils.endereco_normalizer import corrigir_truncados


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


def _smart_title(s: str) -> str:

    def fix_word(w):
        if re.match(r"^[A-Z]{2,3}$", w):
            return w
        if re.match(r"^[A-Z]{2,3}-\d+", w):
            return w
        return w.capitalize()

    return " ".join(fix_word(w) for w in s.split())


def _normalize_cep(cep: str) -> str:
    if not cep:
        return ""

    cep = re.sub(r"\D", "", cep)

    if len(cep) != 8:
        return ""

    return cep


# ============================================================
# 🧱 NORMALIZAÇÃO BASE
# ============================================================

def normalize_base(endereco: str) -> str:
    if not endereco:
        return ""

    s = endereco.strip()

    # remove caracteres invisíveis
    s = re.sub(r"[\u200B-\u200D\uFEFF]", "", s)

    s = re.sub(r"[^\S\r\n]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    s = re.sub(r"\s*,\s*", ", ", s)

    s = re.sub(r",?\s*Brasil$", "", s, flags=re.I)

    return s.strip()


# ============================================================
# 🧭 PARA GEOCODIFICAÇÃO
# ============================================================

def normalize_for_geocoding(endereco: str) -> str:
    if not endereco:
        return ""

    s = normalize_base(endereco)

    s = corrigir_truncados(s)

    s = _smart_title(s)

    s = re.sub(r"\bSta\b", "Santa", s)
    s = re.sub(r"\bSto\b", "Santo", s)

    # complemento expandido
    s = re.sub(
        r"\b(Bloco|Bl|Loja|Lj|Sala|Sl|Apto|Apt|Cj|Conj|Andar|Fundos|Casa|Galpao|Quadra)\s*\w*",
        "",
        s,
        flags=re.IGNORECASE
    )

    s = _limpeza_basica(s)

    return s


# ============================================================
# 🧠 CACHE
# ============================================================

def normalize_for_cache(endereco: str, cep: str = None) -> str:
    if not endereco:
        return ""

    s = normalize_base(endereco)

    s = _remover_acentos(s)
    s = s.upper()

    s = re.sub(r"[^A-Z0-9 ,\-]", "", s)

    cep_norm = _normalize_cep(cep)

    if cep_norm:
        s = f"{s} | CEP:{cep_norm}"

    s = _limpeza_basica(s)

    return s


# ============================================================
# 👁️ DISPLAY
# ============================================================

def normalize_for_display(endereco: str) -> str:
    if not endereco:
        return ""

    return normalize_base(endereco)
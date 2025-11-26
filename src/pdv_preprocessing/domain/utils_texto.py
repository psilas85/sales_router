#sales_router/src/pdv_preprocessing/domain/utils_texto.py

import unicodedata

def fix_encoding(s: str) -> str:
    """
    Corrige problemas de encoding comuns de CSVs exportados por Excel/Windows.
    Mantém acentos. Remove caracteres invisíveis. Normaliza para NFC.
    """
    if not isinstance(s, str):
        return s

    # tenta decodificação Latin1 → UTF-8
    try:
        s = s.encode("latin1").decode("utf-8")
    except Exception:
        pass

    # remove caracteres invisíveis (quebra de encoding)
    s = "".join(c for c in s if ord(c) >= 32)

    # normalização SEM remover acentos
    s = unicodedata.normalize("NFC", s)

    return s.strip()

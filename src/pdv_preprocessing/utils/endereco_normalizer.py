#sales_router/src/pdv_preprocessing/utils/endereco_normalizer.py

# ============================================================
# 📦 src/pdv_preprocessing/utils/endereco_normalizer.py
# 🔧 Módulo unificado de normalização e correção de endereços
# ============================================================

import re
import unicodedata

# ============================================================
# 🔤 DICIONÁRIO DE ABREVIACOES → LOGRADOUROS COMPLETOS
# ============================================================
ABREVIACOES = {

    # ---- Logradouros principais ----
    r"\bav\b": "avenida",
    r"\bav.\b": "avenida",
    r"\bavd\b": "avenida",
    r"\bavd.\b": "avenida",
    r"\bave\b": "avenida",
    r"\bavda\b": "avenida",
    r"\bavn\b": "avenida",

    r"\bal\b": "alameda",
    r"\bal.\b": "alameda",

    r"\br\b": "rua",
    r"\br.\b": "rua",

    r"\btrav\b": "travessa",
    r"\btv\b": "travessa",

    r"\brod\b": "rodovia",
    r"\brotv\b": "rodovia",

    r"\bpc\b": "praca",
    r"\bpr\b": "praca",

    r"\bjd\b": "jardim",
    r"\bjrd\b": "jardim",

    r"\bvil\b": "vila",
    r"\bvl\b": "vila",

    r"\bpq\b": "parque",
    r"\bpk\b": "parque",

    # ---- Títulos e nomes ----
    r"\bdr\b": "doutor",
    r"\bdra\b": "doutora",
    r"\bprof\b": "professor",
    r"\bprofa\b": "professora",
    r"\beng\b": "engenheiro",
    r"\bara\b": "arquiteto",
    r"\bdep\b": "deputado",
    r"\bver\b": "vereador",

    # ---- Complementos ----
    r"\bbl\b": "bloco",
    r"\bap\b": "apartamento",
    r"\bsl\b": "sala",
    r"\bcs\b": "casa",
    r"\bgalp\b": "galpao",
    r"\bkm\b": "quilometro",
}


# ============================================================
# 🔧 Correções de TRUNCAMENTOS
# ============================================================
CORRECOES_TRUNCADOS = {
    "pereira verguei": "pereira vergueiro",
    "elisio t leite": "elisio teixeira leite",
    "manel l americ": "maria luiza americo",
    "yervant kissaji": "yervant kissajian",
    "fiorelli pecciaca": "fiorelli pecciaccia",
    "arm de a pere": "armando de arruda pereira",
    "jacu pessego": "jacu-pessego",
    "fernando m de alme": "fernando mendes de almeida",
    "nogue": "nogueira",
    "verg": "vergueiro",
    "alm psarg": "alameda passageiro",
}


# ============================================================
# 🛠️ NORMALIZAÇÃO GERAL
# ============================================================
def _remover_acentos(s: str) -> str:
    return unicodedata.normalize("NFKD", s).encode("ASCII", "ignore").decode("ASCII")


def expandir_abreviacoes(s: str) -> str:
    for padrao, completo in ABREVIACOES.items():
        s = re.sub(padrao, completo, s)
    return s


def corrigir_truncados(s: str) -> str:
    txt = s.lower()

    for truncado, correto in CORRECOES_TRUNCADOS.items():
        if truncado in txt:
            txt = txt.replace(truncado, correto)

    return txt


# ============================================================
# 🎯 Função principal única
# ============================================================
def normalizar_endereco_completo(endereco: str) -> str:
    """
    Limpa, corrige, expande abreviações e remove acentos.
    Retorna um endereço o mais completo e estável possível.
    """
    if not endereco:
        return ""

    s = endereco.strip().lower()

    # remove acentos
    s = _remover_acentos(s)

    # corrige truncados
    s = corrigir_truncados(s)

    # expande abreviações
    s = expandir_abreviacoes(s)

    # limpeza final
    s = " ".join(s.split())

    return s

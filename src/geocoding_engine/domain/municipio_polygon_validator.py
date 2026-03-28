# sales_router/src/geocoding_engine/domain/municipio_polygon_validator.py

import json
import os
import re
import unicodedata
from pathlib import Path
from functools import lru_cache

from shapely.geometry import shape, Point


BASE_PATH = Path(
    os.getenv(
        "IBGE_MUNICIPIOS_GEOJSON",
        "/app/data/ibge/municipios.geojson"
    )
)


# ============================================================
# NORMALIZAÇÃO
# ============================================================

def _strip_accents(txt: str) -> str:
    txt = unicodedata.normalize("NFKD", txt)
    return "".join(c for c in txt if not unicodedata.combining(c))


def _norm(txt: str | None) -> str | None:
    if not txt:
        return None

    txt = str(txt).strip()

    # remove espaços invisíveis
    txt = re.sub(r"[\u200B-\u200D\uFEFF]", "", txt)

    # normaliza acentos
    txt = _strip_accents(txt)

    # normaliza separadores
    txt = txt.replace("/", " ")
    txt = txt.replace(" - ", " ")
    txt = txt.replace("-", " ")

    # remove pontuação desnecessária no fim
    txt = re.sub(r"[.,;:]+$", "", txt)

    # espaços múltiplos
    txt = re.sub(r"\s+", " ", txt)

    return txt.upper().strip()


def _norm_uf(uf: str | None) -> str | None:
    uf = _norm(uf)

    if not uf:
        return None

    # pega só 2 letras, se possível
    m = re.search(r"\b([A-Z]{2})\b", uf)
    if m:
        return m.group(1)

    return uf if len(uf) == 2 else None


def _norm_cidade(cidade: str | None, uf: str | None = None) -> str | None:
    cidade = _norm(cidade)

    if not cidade:
        return None

    uf_norm = _norm_uf(uf)

    # remove UF grudada no nome da cidade, se vier
    if uf_norm:
        cidade = re.sub(rf"\b{re.escape(uf_norm)}\b$", "", cidade).strip()
        cidade = re.sub(rf"\b{re.escape(uf_norm)}\b", "", cidade).strip()
        cidade = re.sub(r"\s+", " ", cidade).strip()

    return cidade or None


# ============================================================
# LOAD DOS POLÍGONOS
# ============================================================

@lru_cache(maxsize=1)
def _load_polygons():
    if not BASE_PATH.exists():
        raise FileNotFoundError(f"GeoJSON não encontrado: {BASE_PATH}")

    with open(BASE_PATH, "r", encoding="utf-8") as f:
        geo = json.load(f)

    polygons = {}

    for feat in geo.get("features", []):
        props = feat.get("properties", {}) or {}

        cidade = _norm_cidade(
            props.get("NM_MUN")
            or props.get("name")
            or props.get("municipio")
        )

        uf = _norm_uf(
            props.get("SIGLA_UF")
            or props.get("UF")
            or props.get("state")
        )

        if not cidade or not uf:
            continue

        geom = feat.get("geometry")
        if not geom:
            continue

        try:
            polygons[(cidade, uf)] = shape(geom)
        except Exception:
            continue

    return polygons


# ============================================================
# API PÚBLICA
# ============================================================

def ponto_dentro_municipio(
    lat: float,
    lon: float,
    cidade: str | None,
    uf: str | None
) -> bool | None:
    """
    Retornos:
        True  -> ponto validado dentro do município
        False -> ponto válido, mas fora do município / coordenada inválida
        None  -> não foi possível validar (cidade/uf ausentes ou polígono não encontrado)
    """

    # --------------------------------------------------------
    # coordenadas básicas
    # --------------------------------------------------------
    if lat is None or lon is None:
        return False

    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        return False

    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return False

    # --------------------------------------------------------
    # normalização cidade/uf
    # --------------------------------------------------------
    uf_norm = _norm_uf(uf)
    cidade_norm = _norm_cidade(cidade, uf=uf_norm)

    if not cidade_norm or not uf_norm:
        return None

    # --------------------------------------------------------
    # busca polígono
    # --------------------------------------------------------
    polygons = _load_polygons()
    poly = polygons.get((cidade_norm, uf_norm))

    if poly is None:
        return None

    # --------------------------------------------------------
    # validação geométrica
    # contains = interior
    # touches  = aceita ponto exatamente na borda
    # --------------------------------------------------------
    try:
        ponto = Point(lon, lat)
        return bool(poly.contains(ponto) or poly.touches(ponto))
    except Exception:
        return False
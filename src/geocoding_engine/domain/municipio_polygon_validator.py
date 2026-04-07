# sales_router/src/geocoding_engine/domain/municipio_polygon_validator.py

import json
import os
import re
import unicodedata
from pathlib import Path
from functools import lru_cache

from shapely.geometry import shape, Point

BUFFER_GRAUS = float(os.getenv("MUNICIPIO_BUFFER_GRAUS", "0.005"))


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
# LOAD DOS POLÍGONOS (DICT)
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
# LOAD GEOPANDAS (PARA BATCH FAST)
# ============================================================

@lru_cache(maxsize=1)
def carregar_municipios_gdf():
    import geopandas as gpd

    if not BASE_PATH.exists():
        raise FileNotFoundError(f"GeoJSON não encontrado: {BASE_PATH}")

    gdf = gpd.read_file(BASE_PATH)

    # 🔥 ESSAS DUAS LINHAS AQUI
    gdf["cidade"] = gdf["NM_MUN"].apply(_norm_cidade)
    gdf["uf"] = gdf["SIGLA_UF"].apply(_norm_uf)

    return gdf

# ============================================================
# API PÚBLICA
# ============================================================

from loguru import logger

def ponto_dentro_municipio(
    lat: float,
    lon: float,
    cidade: str | None,
    uf: str | None
) -> bool | None:

    if lat is None or lon is None:
        logger.debug(f"[POLYGON][SKIP] lat/lon None | cidade={cidade} uf={uf}")
        return False

    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        logger.debug(f"[POLYGON][SKIP] lat/lon inválido | lat={lat} lon={lon}")
        return False

    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        logger.debug(f"[POLYGON][SKIP] fora range global | lat={lat} lon={lon}")
        return False

    uf_norm = _norm_uf(uf)
    cidade_norm = _norm_cidade(cidade, uf=uf_norm)

    if not cidade_norm or not uf_norm:
        logger.debug(f"[POLYGON][SKIP] cidade/uf inválidos | cidade={cidade} uf={uf}")
        return None

    polygons = _load_polygons()
    poly = polygons.get((cidade_norm, uf_norm))

    if poly is None:
        logger.warning(
            f"[POLYGON][NOT_FOUND] cidade_norm={cidade_norm} uf_norm={uf_norm} "
            f"| original={cidade}/{uf}"
        )
        return None

    try:
        ponto = Point(lon, lat)

        inside_strict = poly.contains(ponto)
        inside_buffer = poly.buffer(BUFFER_GRAUS).contains(ponto)

        logger.info(
            f"[POLYGON][CHECK] lat={lat} lon={lon} "
            f"| cidade={cidade_norm}-{uf_norm} "
            f"| strict={inside_strict} buffer={inside_buffer}"
        )

        if inside_strict:
            return True

        if inside_buffer:
            logger.warning(
                f"[POLYGON][BUFFER_HIT] ponto aceito por tolerância "
                f"| lat={lat} lon={lon} cidade={cidade_norm}-{uf_norm}"
            )
            return True

        logger.warning(
            f"[POLYGON][OUTSIDE] fora do município "
            f"| lat={lat} lon={lon} cidade={cidade_norm}-{uf_norm}"
        )

        return False

    except Exception as e:
        logger.error(f"[POLYGON][ERRO] {e}")
        return None
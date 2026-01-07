#sales_router/src/pdv_preprocessing/domain/capital_polygon_validator.py

# ============================================================
# 📦 capital_polygon_validator.py
# ============================================================

import json
from pathlib import Path
from shapely.geometry import shape, Point
from functools import lru_cache
import unicodedata

BASE_PATH = Path("data/ibge/capitais.geojson")

def _norm(txt: str | None) -> str | None:
    if not txt:
        return None
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    return txt.upper().strip()

@lru_cache(maxsize=1)
def _load_polygons():
    """
    Carrega capitais.geojson uma única vez
    Retorna dict: {(CIDADE, UF): shapely_polygon}
    """
    with open(BASE_PATH, "r", encoding="utf-8") as f:
        geo = json.load(f)

    polygons = {}

    for feat in geo["features"]:
        props = feat["properties"]
        cidade = _norm(props.get("NM_MUN"))
        uf = _norm(props.get("SIGLA_UF"))

        if not cidade or not uf:
            continue

        polygons[(cidade, uf)] = shape(feat["geometry"])

    return polygons


def ponto_dentro_capital(lat: float, lon: float, cidade: str | None, uf: str | None) -> bool:
    """
    Retorna True se ponto estiver dentro do polígono da capital.
    Se cidade/UF não forem capitais → False
    """
    if lat is None or lon is None:
        return False

    cidade = _norm(cidade)
    uf = _norm(uf)

    if not cidade or not uf:
        return False

    polygons = _load_polygons()
    poly = polygons.get((cidade, uf))

    if not poly:
        return False

    ponto = Point(lon, lat)
    return poly.contains(ponto)

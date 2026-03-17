#sales_router/src/geocoding_engine/domain/municipio_polygon_validator.py

import json
import os
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

def _norm(txt: str | None) -> str | None:
    if not txt:
        return None
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))
    return txt.upper().strip()


@lru_cache(maxsize=1)
def _load_polygons():
    if not BASE_PATH.exists():
        raise FileNotFoundError(f"GeoJSON não encontrado: {BASE_PATH}")

    with open(BASE_PATH, "r", encoding="utf-8") as f:
        geo = json.load(f)

    polygons = {}

    for feat in geo.get("features", []):
        props = feat.get("properties", {})

        cidade = _norm(
            props.get("NM_MUN")
            or props.get("name")
            or props.get("municipio")
        )
        uf = _norm(
            props.get("SIGLA_UF")
            or props.get("UF")
            or props.get("state")
        )

        if not cidade or not uf:
            continue

        try:
            polygons[(cidade, uf)] = shape(feat["geometry"])
        except Exception:
            continue

    return polygons


def ponto_dentro_municipio(
    lat: float,
    lon: float,
    cidade: str | None,
    uf: str | None
) -> bool | None:
    if lat is None or lon is None:
        return False

    cidade = _norm(cidade)
    uf = _norm(uf)

    if not cidade or not uf:
        return None

    polygons = _load_polygons()
    poly = polygons.get((cidade, uf))

    if not poly:
        return None

    ponto = Point(lon, lat)
    return poly.intersects(ponto)
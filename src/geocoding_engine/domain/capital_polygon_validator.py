#sales_router/src/geocoding_engine/domain/capital_polygon_validator.py

import json
from pathlib import Path
from shapely.geometry import shape, Point
from shapely.prepared import prep
from functools import lru_cache
import unicodedata

# ------------------------------------------------------------
# 📁 PATH ROBUSTO
# ------------------------------------------------------------
BASE_PATH = Path(__file__).resolve().parent.parent.parent / "data/ibge/capitais.geojson"


# ------------------------------------------------------------
# 🔤 NORMALIZAÇÃO
# ------------------------------------------------------------
def _norm(txt: str | None) -> str | None:
    if not txt:
        return None

    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(c for c in txt if not unicodedata.combining(c))

    return txt.upper().strip()


# ------------------------------------------------------------
# 🚀 LOAD + CACHE (COM PREPARED GEOMETRY)
# ------------------------------------------------------------
@lru_cache(maxsize=1)
def _load_polygons():
    """
    Carrega capitais.geojson uma única vez
    Retorna dict: {(CIDADE, UF): prepared_polygon}
    """

    if not BASE_PATH.exists():
        raise FileNotFoundError(f"GeoJSON não encontrado: {BASE_PATH}")

    with open(BASE_PATH, "r", encoding="utf-8") as f:
        geo = json.load(f)

    polygons = {}

    for feat in geo["features"]:
        props = feat["properties"]

        cidade = _norm(props.get("NM_MUN"))
        uf = _norm(props.get("SIGLA_UF"))

        if not cidade or not uf:
            continue

        geom = shape(feat["geometry"])

        # 🔥 prepared geometry (muito mais rápido)
        polygons[(cidade, uf)] = prep(geom)

    return polygons


# ------------------------------------------------------------
# 🎯 VALIDAÇÃO
# ------------------------------------------------------------
def ponto_dentro_capital(
    lat: float,
    lon: float,
    cidade: str | None,
    uf: str | None
) -> bool:
    """
    Retorna True se ponto estiver dentro do polígono da capital.

    ✔ Usa prepared geometry (rápido)
    ✔ Trata borda (buffer)
    ✔ Seguro para produção
    """

    # --------------------------------------------------------
    # validação básica
    # --------------------------------------------------------
    if lat is None or lon is None:
        return False

    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
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

    # --------------------------------------------------------
    # 🔥 VALIDAÇÃO COM BORDA
    # --------------------------------------------------------
    try:
        if poly.contains(ponto):
            return True

        # fallback borda
        if poly.context.buffer(0.00001).contains(ponto):
            return True

        return False

    except Exception:
        return False
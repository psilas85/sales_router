# sales_router/src/geocoding_engine/domain/geo_validator.py

# ============================================================
# 📦 geo_validator.py
# Validação geográfica leve (pipeline)
# ============================================================

from geocoding_engine.config.uf_bounds import UF_BOUNDS
import pandas as pd


class GeoValidator:
    """
    Validação geográfica leve para uso DURANTE o pipeline.

    Objetivo:
        - Filtrar lixo rapidamente
        - NÃO usar validação pesada (polígono)

    Etapas:

        1️⃣ Coordenada válida
        2️⃣ Bounds da UF
        3️⃣ Range geográfico global

    Retornos:

        ok
        falha
        fora_uf
    """

    @staticmethod
    def validar_ponto(lat, lon, cidade, uf):

        # -----------------------------------------------------
        # 1. VALIDAÇÃO BÁSICA
        # -----------------------------------------------------
        if lat is None or lon is None:
            return "falha"

        try:
            lat = float(lat)
            lon = float(lon)
        except Exception:
            return "falha"

        # range global
        if not (-90 <= lat <= 90 and -180 <= lon <= 180):
            return "falha"

        # -----------------------------------------------------
        # 2. UF BOUNDS
        # -----------------------------------------------------
        uf = (uf or "").upper().strip()

        bounds = UF_BOUNDS.get(uf)

        if bounds:
            if not (
                bounds["lat_min"] <= lat <= bounds["lat_max"]
                and bounds["lon_min"] <= lon <= bounds["lon_max"]
            ):
                return "fora_uf"

        # -----------------------------------------------------
        # OK
        # -----------------------------------------------------
        return "ok"


# ============================================================
# 🚀 BATCH OTIMIZADO (GEOPANDAS)
# ============================================================

def validar_municipios_batch_fast(df, gdf_municipios):

    from shapely.geometry import Point
    from shapely.prepared import prep
    from geocoding_engine.domain.municipio_polygon_validator import (
        BUFFER_GRAUS,
        _load_polygons,
        _norm_cidade,
        _norm_uf,
    )

    # 🔥 GARANTE COLUNAS
    df["cidade"] = [
        _norm_cidade(cidade, uf=uf)
        for cidade, uf in zip(df["cidade"], df["uf"])
    ]
    df["uf"] = [_norm_uf(uf) for uf in df["uf"]]

    polygons = _load_polygons()
    prepared_cache = {}
    validos = []

    for row in df.itertuples(index=False):
        cidade = getattr(row, "cidade", None)
        uf = getattr(row, "uf", None)
        lat = getattr(row, "lat", None)
        lon = getattr(row, "lon", None)

        if not cidade or not uf or pd.isnull(lat) or pd.isnull(lon):
            validos.append(False)
            continue

        poly_key = (cidade, uf)
        poly = polygons.get(poly_key)

        if poly is None:
            validos.append(False)
            continue

        if poly_key not in prepared_cache:
            prepared_cache[poly_key] = (
                prep(poly),
                prep(poly.buffer(BUFFER_GRAUS)),
            )

        strict_poly, buffer_poly = prepared_cache[poly_key]
        point = Point(float(lon), float(lat))

        validos.append(strict_poly.contains(point) or buffer_poly.contains(point))

    df["valido_municipio"] = validos

    return df

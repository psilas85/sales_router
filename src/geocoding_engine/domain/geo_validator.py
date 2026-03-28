# sales_router/src/geocoding_engine/domain/geo_validator.py

# ============================================================
# 📦 geo_validator.py
# Validação geográfica leve (pipeline)
# ============================================================

from geocoding_engine.config.uf_bounds import UF_BOUNDS


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
# 🧠 VALIDAÇÃO PESADA (USAR FORA DO PIPELINE)
# ============================================================

def validar_municipio(lat, lon, cidade, uf):
    """
    Validação pesada usando polígono IBGE.

    ⚠️ NÃO usar dentro do pipeline principal.
    ⚠️ Usar apenas em pós-processamento (batch).
    """

    from geocoding_engine.domain.municipio_polygon_validator import (
        ponto_dentro_municipio
    )

    if lat is None or lon is None:
        return False

    if not cidade or not uf:
        return False

    try:
        return ponto_dentro_municipio(lat, lon, cidade, uf)
    except Exception:
        return False


# ============================================================
# 🚀 BATCH (PARA SPREADSHEET / FINAL)
# ============================================================

def validar_municipios_batch(df):
    """
    Validação em lote (simples).
    """

    resultados = []

    for _, row in df.iterrows():

        lat = row.get("lat")
        lon = row.get("lon")
        cidade = row.get("cidade")
        uf = row.get("uf")

        ok = validar_municipio(lat, lon, cidade, uf)

        resultados.append(ok)

    df["valido_municipio"] = resultados

    return df


# ============================================================
# 🚀 BATCH OTIMIZADO (GEOPANDAS)
# ============================================================

def validar_municipios_batch_fast(df, gdf_municipios):
    """
    Validação em lote otimizada (recomendado para grandes volumes).

    Requisitos:
        - geopandas
        - shapely
        - gdf_municipios carregado previamente
    """

    import geopandas as gpd
    from shapely.geometry import Point

    # cria geometria
    geometry = [
        Point(lon, lat) if lat is not None and lon is not None else None
        for lat, lon in zip(df["lat"], df["lon"])
    ]

    gdf = gpd.GeoDataFrame(df.copy(), geometry=geometry, crs="EPSG:4326")

    # spatial join
    joined = gpd.sjoin(
        gdf,
        gdf_municipios,
        how="left",
        predicate="within"
    )

    df["valido_municipio"] = joined.index_right.notnull()

    return df
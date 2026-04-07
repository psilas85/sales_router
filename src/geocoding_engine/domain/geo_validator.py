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

    import geopandas as gpd
    from shapely.geometry import Point

    # 🔥 GARANTE COLUNAS
    df["cidade"] = df["cidade"].astype(str).str.upper().str.strip()
    df["uf"] = df["uf"].astype(str).str.upper().str.strip()

    # 🔥 cria geometria
    geometry = [
        Point(lon, lat) if pd.notnull(lat) and pd.notnull(lon) else None
        for lat, lon in zip(df["lat"], df["lon"])
    ]

    gdf = gpd.GeoDataFrame(df.copy(), geometry=geometry, crs="EPSG:4326")

    # 🔥 join
    joined = gpd.sjoin(
        gdf,
        gdf_municipios,
        how="left",
        predicate="within"
    )

    print("[DEBUG JOIN COLS]", joined.columns.tolist())

    # 🔥 VALIDAÇÃO CORRETA (SEM ADIVINHAÇÃO)
    df["valido_municipio"] = (
        joined["index_right"].notnull() &
        (joined["cidade_left"] == joined["cidade_right"]) &
        (joined["uf_left"] == joined["uf_right"])
    )

    return df
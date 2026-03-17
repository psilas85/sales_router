#sales_router/src/geocoding_engine/domain/geo_validator.py

# ============================================================
# 📦 geo_validator.py
# Validação geográfica de coordenadas
# ============================================================

from geocoding_engine.domain.municipio_polygon_validator import (
    ponto_dentro_municipio
)
from geocoding_engine.config.uf_bounds import UF_BOUNDS


class GeoValidator:
    """
    Valida se uma coordenada geográfica é plausível
    para uma determinada cidade / UF.

    Etapas de validação:

        1️⃣ Coordenada válida
        2️⃣ UF bounds
        3️⃣ Polígono IBGE do município

    Retornos possíveis:

        ok
        falha
        fora_uf
        fora_municipio
    """

    @staticmethod
    def validar_ponto(lat, lon, cidade, uf):

        # -----------------------------------------------------
        # Coordenada nula
        # -----------------------------------------------------

        if lat is None or lon is None:
            return "falha"

        # -----------------------------------------------------
        # Normalização UF
        # -----------------------------------------------------

        uf = (uf or "").upper().strip()

        # -----------------------------------------------------
        # Validação bounding box UF
        # -----------------------------------------------------

        bounds = UF_BOUNDS.get(uf)

        if bounds:

            lat_min = bounds["lat_min"]
            lat_max = bounds["lat_max"]
            lon_min = bounds["lon_min"]
            lon_max = bounds["lon_max"]

            if not (
                lat_min <= lat <= lat_max
                and lon_min <= lon <= lon_max
            ):
                return "fora_uf"

        # -----------------------------------------------------
        # Validação município (IBGE polygon)
        # -----------------------------------------------------

        dentro = ponto_dentro_municipio(
            lat,
            lon,
            cidade,
            uf
        )

        # False = ponto fora do polígono
        if dentro is False:
            return "fora_municipio"

        # None = município não encontrado → não invalida
        return "ok"
#sales_router/src/geocoding_engine/domain/geo_validator.py

from geocoding_engine.domain.municipio_polygon_validator import ponto_dentro_municipio
from geocoding_engine.config.uf_bounds import UF_BOUNDS


class GeoValidator:

    @staticmethod
    def validar_ponto(lat, lon, cidade, uf):

        if lat is None or lon is None:
            return "falha"

        bounds = UF_BOUNDS.get(uf)

        if bounds:
            if not (
                bounds["lat_min"] <= lat <= bounds["lat_max"]
                and bounds["lon_min"] <= lon <= bounds["lon_max"]
            ):
                return "fora_uf"

        dentro = ponto_dentro_municipio(lat, lon, cidade, uf)

        if dentro is False:
            return "fora_municipio"

        return "ok"
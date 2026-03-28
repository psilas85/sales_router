#sales_router/src/geocoding_engine/domain/utils_geo.py

from geopy.distance import geodesic

CEP_INVALIDO_LIST = set()


def cep_invalido(cep: str) -> bool:
    if not cep:
        return True

    cep = str(cep).replace("-", "").strip()

    if not cep.isdigit():
        return True

    if len(cep) != 8:
        return True

    if cep == "00000000":
        return True

    if cep in CEP_INVALIDO_LIST:
        return True

    return False


def coordenada_generica(lat: float, lon: float) -> bool:
    """
    Validação leve de coordenada.
    NÃO deve rejeitar coordenadas válidas.
    """

    if lat is None or lon is None:
        return True

    try:
        lat = float(lat)
        lon = float(lon)
    except Exception:
        return True

    # inválido global
    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return True

    # coordenada zero
    if abs(lat) < 0.0001 and abs(lon) < 0.0001:
        return True

    return False
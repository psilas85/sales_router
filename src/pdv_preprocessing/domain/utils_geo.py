# ============================================================
# 📦 src/pdv_preprocessing/domain/utils_geo.py
# ============================================================

from geopy.distance import geodesic

CEP_INVALIDO_LIST = set()  # recomendado: deixar vazio

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

    # Apenas os realmente problemáticos (se quiser manter)
    if cep in CEP_INVALIDO_LIST:
        return True

    return False


def coordenada_generica(lat: float, lon: float) -> bool:

    if lat is None or lon is None:
        return True

    if abs(lat) < 0.0001 and abs(lon) < 0.0001:
        return True

    if not (-90 <= lat <= 90 and -180 <= lon <= 180):
        return True

    # Fallback Limeira (Nominatim antigo)
    if geodesic((lat, lon), (-22.563, -47.401)).km < 5:
        return True

    # Fallback Centro do Brasil (Google)
    if geodesic((lat, lon), (-14.235004, -51.92528)).km < 10:
        return True

    return False

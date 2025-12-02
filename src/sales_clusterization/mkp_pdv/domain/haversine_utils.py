import math

def haversine(lat1, lon1, lat2, lon2):
    """
    Retorna a distância em km entre dois pontos (lat/lon).
    Fórmula de Haversine — mesma usada no módulo CEP.
    """
    R = 6371.0  # raio da Terra em km

    lat1 = math.radians(float(lat1))
    lon1 = math.radians(float(lon1))
    lat2 = math.radians(float(lat2))
    lon2 = math.radians(float(lon2))

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))

    return R * c

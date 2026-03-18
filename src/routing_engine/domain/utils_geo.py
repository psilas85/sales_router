#sales_router/src/routing_engine/domain/utils_geo.py

import math
from typing import Iterable, Tuple


BRAZIL_BOUNDS = {
    "min_lat": -34.0,
    "max_lat": 5.5,
    "min_lon": -74.5,
    "max_lon": -28.0,
}


def is_valid_lat_lon(lat: float, lon: float) -> bool:
    if lat is None or lon is None:
        return False

    try:
        lat = float(lat)
        lon = float(lon)
    except (TypeError, ValueError):
        return False

    return (
        BRAZIL_BOUNDS["min_lat"] <= lat <= BRAZIL_BOUNDS["max_lat"]
        and BRAZIL_BOUNDS["min_lon"] <= lon <= BRAZIL_BOUNDS["max_lon"]
    )


def normalize_coord(lat: float, lon: float) -> Tuple[float, float]:
    """
    Corrige inversão lat/lon quando detectável.
    Regra prática Brasil:
    - latitude entre -34 e +5.5
    - longitude entre -74.5 e -28
    """
    lat = float(lat)
    lon = float(lon)

    if is_valid_lat_lon(lat, lon):
        return lat, lon

    if is_valid_lat_lon(lon, lat):
        return lon, lat

    return lat, lon


def haversine_km(a: tuple[float, float], b: tuple[float, float]) -> float:
    r = 6371.0
    lat1, lon1 = map(math.radians, a)
    lat2, lon2 = map(math.radians, b)

    dlat = lat2 - lat1
    dlon = lon2 - lon1

    h = (
        math.sin(dlat / 2) ** 2
        + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
    )
    return 2 * r * math.asin(math.sqrt(h))


def mean_center(coords: Iterable[tuple[float, float]]) -> tuple[float, float]:
    coords = list(coords)
    if not coords:
        raise ValueError("Lista de coordenadas vazia para cálculo do centro.")

    lat_mean = sum(c[0] for c in coords) / len(coords)
    lon_mean = sum(c[1] for c in coords) / len(coords)
    return float(lat_mean), float(lon_mean)
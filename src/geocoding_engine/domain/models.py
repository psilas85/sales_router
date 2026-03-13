#sales_router/src/geocoding_engine/domain/models.py

from dataclasses import dataclass


@dataclass
class AddressInput:

    id: int
    address: str


@dataclass
class GeocodeResult:

    id: int
    lat: float | None
    lon: float | None
    source: str
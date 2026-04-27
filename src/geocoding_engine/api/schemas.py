#sales_router/src/geocoding_engine/api/schemas.py

from typing import List

from pydantic import BaseModel


class GeocodeRequest(BaseModel):
    id: int | str | None = None
    endereco: str | None = None
    address: str | None = None
    logradouro: str | None = None
    numero: str | None = ""
    bairro: str | None = None
    cidade: str
    uf: str
    cep: str | None = None


class GeocodeResponse(BaseModel):
    lat: float | None
    lon: float | None
    status: str


class GeocodeBatchRequest(BaseModel):
    addresses: List[GeocodeRequest]

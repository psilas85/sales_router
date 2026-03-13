#sales_router/src/geocoding_engine/api/schemas.py

from pydantic import BaseModel


class GeocodeRequest(BaseModel):
    endereco: str
    cidade: str
    uf: str


class GeocodeResponse(BaseModel):
    lat: float | None
    lon: float | None
    status: str
#sales_router/src/routing_engine/insfrastructure/database_writer.py

from __future__ import annotations

import json
from datetime import datetime


from routing_engine.infrastructure.database_connection import get_db_connection


class DatabaseWriter:
    def __init__(self):
        self.conn = get_db_connection()
        self.conn.autocommit = True

    def gravar_route_cache(
        self,
        origem_lat: float,
        origem_lon: float,
        destino_lat: float,
        destino_lon: float,
        distancia_km: float,
        tempo_min: float,
        fonte: str,
        rota_coord=None,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO route_cache (
                    origem_lat,
                    origem_lon,
                    destino_lat,
                    destino_lon,
                    distancia_km,
                    tempo_min,
                    fonte,
                    atualizado_em,
                    rota_coord
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (origem_lat, origem_lon, destino_lat, destino_lon)
                DO UPDATE SET
                    distancia_km = EXCLUDED.distancia_km,
                    tempo_min = EXCLUDED.tempo_min,
                    fonte = EXCLUDED.fonte,
                    atualizado_em = EXCLUDED.atualizado_em,
                    rota_coord = EXCLUDED.rota_coord
                """,
                (
                    origem_lat,
                    origem_lon,
                    destino_lat,
                    destino_lon,
                    float(distancia_km),
                    float(tempo_min),
                    fonte,
                    datetime.now(),
                    json.dumps(rota_coord or []),
                ),
            )

    def close(self):
        if self.conn:
            self.conn.close()
#sales_router/src/routing_engine/insfrastructure/database_reader.py

from __future__ import annotations

import json
from typing import Optional, Dict, Any

from psycopg2.extras import RealDictCursor

from routing_engine.infrastructure.database_connection import get_db_connection


class DatabaseReader:
    def __init__(self):
        self.conn = get_db_connection()
        self.conn.autocommit = True

    def buscar_route_cache(
        self,
        origem_lat: float,
        origem_lon: float,
        destino_lat: float,
        destino_lon: float,
    ) -> Optional[Dict[str, Any]]:
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT
                    origem_lat,
                    origem_lon,
                    destino_lat,
                    destino_lon,
                    distancia_km,
                    tempo_min,
                    fonte,
                    atualizado_em,
                    rota_coord
                FROM route_cache
                WHERE origem_lat = %s
                  AND origem_lon = %s
                  AND destino_lat = %s
                  AND destino_lon = %s
                """,
                (
                    origem_lat,
                    origem_lon,
                    destino_lat,
                    destino_lon,
                ),
            )

            row = cur.fetchone()
            if not row:
                return None

            if row.get("rota_coord") and isinstance(row["rota_coord"], str):
                try:
                    row["rota_coord"] = json.loads(row["rota_coord"])
                except Exception:
                    row["rota_coord"] = []

            return dict(row)

    def close(self):
        if self.conn:
            self.conn.close()
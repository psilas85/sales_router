#sales_router/src/routing_engine/insfrastructure/routing_history_repository.py

from __future__ import annotations

from typing import Optional

from routing_engine.infrastructure.database_connection import get_db_connection


class RoutingHistoryRepository:
    def __init__(self):
        self.conn = get_db_connection()
        self.conn.autocommit = True

    def salvar_historico(
        self,
        request_id: str,
        tenant_id: int,
        origem: Optional[str],
        total_pdvs: int,
        total_grupos: int,
        total_rotas: int,
        cache_hits: int,
        osrm_hits: int,
        google_hits: int,
        haversine_hits: int,
        tempo_execucao_ms: int,
    ) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO routing_historico (
                    request_id,
                    tenant_id,
                    origem,
                    total_pdvs,
                    total_grupos,
                    total_rotas,
                    cache_hits,
                    osrm_hits,
                    google_hits,
                    haversine_hits,
                    tempo_execucao_ms
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    request_id,
                    tenant_id,
                    origem,
                    total_pdvs,
                    total_grupos,
                    total_rotas,
                    cache_hits,
                    osrm_hits,
                    google_hits,
                    haversine_hits,
                    tempo_execucao_ms,
                ),
            )

    def close(self):
        if self.conn:
            self.conn.close()
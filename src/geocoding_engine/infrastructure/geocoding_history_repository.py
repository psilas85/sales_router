#sales_router/src/geocoding_engine/infrastructure/geocoding_history_repository.py

class GeocodingHistoryRepository:

    def __init__(self, conn):
        self.conn = conn

    def salvar(
        self,
        request_id,
        tenant_id,
        origem,
        total,
        sucesso,
        falhas,
        cache_hits,
        nominatim_hits,
        google_hits,
        tempo_ms,
    ):
        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO geocoding_historico (
                        request_id,
                        tenant_id,
                        origem,
                        total_enderecos,
                        sucesso,
                        falhas,
                        cache_hits,
                        nominatim_hits,
                        google_hits,
                        tempo_execucao_ms
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                    """,
                    (
                        request_id,
                        tenant_id,
                        origem,
                        total,
                        sucesso,
                        falhas,
                        cache_hits,
                        nominatim_hits,
                        google_hits,
                        tempo_ms,
                    ),
                )

            self.conn.commit()

        except Exception:
            self.conn.rollback()
            raise
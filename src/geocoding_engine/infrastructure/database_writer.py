#sales_router/src/geocoding_engine/infrastructure/database_writer.py

class DatabaseWriter:

    def __init__(self, conn):
        self.conn = conn

    def salvar_cache(self, endereco, lat, lon, origem):

        try:

            with self.conn.cursor() as cur:

                cur.execute(
                    """
                    INSERT INTO enderecos_cache (
                        endereco,
                        lat,
                        lon,
                        origem
                    )
                    VALUES (%s,%s,%s,%s)
                    ON CONFLICT (endereco)
                    DO UPDATE SET
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        origem = EXCLUDED.origem,
                        atualizado_em = NOW()
                    """,
                    (endereco, lat, lon, origem)
                )

            self.conn.commit()

        except Exception:

            self.conn.rollback()
            raise

    def atualizar_cache(self, id, lat, lon):

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE enderecos_cache
                    SET 
                        lat = %s,
                        lon = %s,
                        origem = 'manual_edit',
                        atualizado_em = NOW()
                    WHERE id = %s
                    """,
                    (lat, lon, id)
                )

            self.conn.commit()

        except Exception:
            self.conn.rollback()
            raise
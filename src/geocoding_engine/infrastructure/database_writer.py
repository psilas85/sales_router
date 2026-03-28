#sales_router/src/geocoding_engine/infrastructure/database_writer.py

from geocoding_engine.domain.address_normalizer import normalize_for_cache
from loguru import logger


class DatabaseWriter:

    def __init__(self, conn):
        self.conn = conn

    # ---------------------------------------------------------
    # 🔥 SALVAR CACHE
    # ---------------------------------------------------------
    def salvar_cache(self, endereco, lat, lon, origem):

        if not endereco:
            return

        try:

            endereco_norm = normalize_for_cache(endereco)

            with self.conn.cursor() as cur:

                cur.execute(
                    """
                    INSERT INTO enderecos_cache (
                        endereco,
                        endereco_normalizado,
                        lat,
                        lon,
                        origem
                    )
                    VALUES (%s,%s,%s,%s,%s)
                    ON CONFLICT (endereco_normalizado)
                    DO UPDATE SET
                        lat = EXCLUDED.lat,
                        lon = EXCLUDED.lon,
                        origem = EXCLUDED.origem,
                        atualizado_em = NOW()
                    """,
                    (endereco, endereco_norm, lat, lon, origem)
                )

            self.conn.commit()

        except Exception as e:
            logger.error(f"[CACHE_SAVE_ERRO] {e}")
            self.conn.rollback()
            raise

    # ---------------------------------------------------------
    # 🔧 UPDATE MANUAL
    # ---------------------------------------------------------
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

        except Exception as e:
            logger.error(f"[CACHE_UPDATE_ERRO] {e}")
            self.conn.rollback()
            raise
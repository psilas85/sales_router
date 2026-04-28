#sales_router/src/geocoding_engine/infrastructure/database_writer.py

from loguru import logger
from geocoding_engine.domain.cache_key_builder import build_cache_key, build_canonical_address


class DatabaseWriter:

    def __init__(self, conn):
        self.conn = conn

    def salvar_cache(
        self,
        logradouro,
        numero,
        cidade,
        uf,
        endereco_original,
        lat,
        lon,
        origem
    ):

        if not logradouro or not numero or not cidade or not uf:
            return

        try:

            endereco_canonico = build_canonical_address(
                logradouro,
                numero,
                cidade,
                uf,
            )
            endereco_norm = build_cache_key(
                logradouro,
                numero,
                cidade,
                uf
            )

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
                    (
                        endereco_canonico,
                        endereco_norm,
                        lat,
                        lon,
                        origem
                    )
                )

            self.conn.commit()

        except Exception as e:
            logger.error(f"[CACHE_SAVE_ERRO] {e}")
            self.conn.rollback()
            raise

    # ---------------------------------------------------------
    # 🔧 UPDATE MANUAL
    # ---------------------------------------------------------
    def criar_cache_manual(self, endereco, endereco_normalizado, lat, lon, origem="manual_create"):

        try:

            with self.conn.cursor() as cur:

                cur.execute(
                    """
                    INSERT INTO enderecos_cache (
                        endereco,
                        endereco_normalizado,
                        lat,
                        lon,
                        origem,
                        criado_em,
                        atualizado_em
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW(), NOW())
                    RETURNING id
                    """,
                    (endereco, endereco_normalizado, lat, lon, origem)
                )

                row = cur.fetchone()

            self.conn.commit()
            return row[0] if row else None

        except Exception as e:
            logger.error(f"[CACHE_CREATE_ERRO] {e}")
            self.conn.rollback()
            raise

    def atualizar_cache(self, id, lat, lon, endereco=None, endereco_normalizado=None, origem="manual_edit"):

        try:

            with self.conn.cursor() as cur:

                if endereco is not None and endereco_normalizado is not None:
                    cur.execute(
                        """
                        UPDATE enderecos_cache
                        SET
                            endereco = %s,
                            endereco_normalizado = %s,
                            lat = %s,
                            lon = %s,
                            origem = %s,
                            atualizado_em = NOW()
                        WHERE id = %s
                        """,
                        (endereco, endereco_normalizado, lat, lon, origem, id)
                    )
                else:
                    cur.execute(
                        """
                        UPDATE enderecos_cache
                        SET 
                            lat = %s,
                            lon = %s,
                            origem = %s,
                            atualizado_em = NOW()
                        WHERE id = %s
                        """,
                        (lat, lon, origem, id)
                    )

                if cur.rowcount == 0:
                    raise ValueError("Cache não encontrado")

            self.conn.commit()

        except Exception as e:
            logger.error(f"[CACHE_UPDATE_ERRO] {e}")
            self.conn.rollback()
            raise

    def excluir_cache(self, id):

        try:
            with self.conn.cursor() as cur:
                cur.execute(
                    """
                    DELETE FROM enderecos_cache
                    WHERE id = %s
                    """,
                    (id,)
                )

                if cur.rowcount == 0:
                    raise ValueError("Cache não encontrado")

            self.conn.commit()

        except Exception as e:
            logger.error(f"[CACHE_DELETE_ERRO] {e}")
            self.conn.rollback()
            raise
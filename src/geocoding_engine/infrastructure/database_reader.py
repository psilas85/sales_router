#sales_router/src/geocoding_engine/infrastructure/database_reader.py

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from loguru import logger

from geocoding_engine.domain.address_normalizer import normalize_for_cache


class DatabaseReader:

    def __init__(self):
        self.conn = None
        self._connect()

    # ---------------------------------------------------------
    # 🔥 CONEXÃO RESILIENTE
    # ---------------------------------------------------------
    def _connect(self):

        try:
            self.conn = psycopg2.connect(
                dbname=os.getenv("DB_NAME"),
                user=os.getenv("DB_USER"),
                password=os.getenv("DB_PASSWORD"),
                host=os.getenv("DB_HOST"),
                port=os.getenv("DB_PORT"),
            )
            self.conn.autocommit = True
            logger.info("[DB] conectado")

        except Exception as e:
            logger.error(f"[DB][CONNECT_ERROR] {e}")
            raise

    def _ensure_connection(self):

        try:
            if self.conn is None or self.conn.closed != 0:
                logger.warning("[DB] reconectando...")
                self._connect()

        except Exception as e:
            logger.error(f"[DB][RECONNECT_ERROR] {e}")
            self._connect()

    # ---------------------------------------------------------
    # CACHE GLOBAL INDIVIDUAL
    # ---------------------------------------------------------
    def buscar_cache(self, endereco):

        if not endereco:
            return None

        self._ensure_connection()

        endereco_norm = normalize_for_cache(endereco)

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute(
                    """
                    SELECT lat, lon
                    FROM enderecos_cache
                    WHERE endereco_normalizado = %s
                    LIMIT 1
                    """,
                    (endereco_norm,),
                )

                row = cur.fetchone()

                if row:
                    return row["lat"], row["lon"]

        except Exception as e:
            logger.warning(f"[CACHE][ERRO] {e}")

        return None

    # ---------------------------------------------------------
    # CACHE GLOBAL EM LOTE
    # ---------------------------------------------------------
    def buscar_cache_em_lote(self, enderecos):

        if not enderecos:
            return {}

        self._ensure_connection()

        # 🔥 normalização única e consistente
        enderecos_norm = [
            normalize_for_cache(e)
            for e in enderecos
            if e
        ]

        if not enderecos_norm:
            return {}

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute(
                    """
                    SELECT endereco_normalizado, lat, lon
                    FROM enderecos_cache
                    WHERE endereco_normalizado = ANY(%s)
                    """,
                    (enderecos_norm,),
                )

                rows = cur.fetchall()

            return {
                r["endereco_normalizado"]: (r["lat"], r["lon"])
                for r in rows
            }

        except Exception as e:
            logger.warning(f"[CACHE_LOTE][ERRO] {e}")
            return {}

    # ---------------------------------------------------------
    # BUSCA FILTRADA (UI / edição manual)
    # ---------------------------------------------------------
    def buscar_cache_filtrado(self, cidade, uf, endereco=None, limit=50):

        self._ensure_connection()

        try:
            termos = normalize_for_cache(
                f"{cidade} {uf} {endereco or ''}"
            ).split()

            if not termos:
                return []

            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:

                where_clauses = []
                params = []

                for termo in termos:
                    where_clauses.append("endereco_normalizado ILIKE %s")
                    params.append(f"%{termo}%")

                where_sql = " AND ".join(where_clauses)

                query = f"""
                SELECT id, endereco, lat, lon, origem, atualizado_em
                FROM enderecos_cache
                WHERE {where_sql}
                ORDER BY atualizado_em DESC
                LIMIT %s
                """

                params.append(limit)

                cur.execute(query, tuple(params))
                return cur.fetchall()

        except Exception as e:
            logger.warning(f"[CACHE_FILTRO][ERRO] {e}")
            return []
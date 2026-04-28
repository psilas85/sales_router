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
   

    def buscar_cache(self, cache_key):

        if not cache_key:
            return None

        self._ensure_connection()

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute(
                    """
                    SELECT lat, lon
                    FROM enderecos_cache
                    WHERE endereco_normalizado = %s
                    LIMIT 1
                    """,
                    (cache_key,),
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
    def buscar_cache_em_lote(self, cache_keys):

        if not cache_keys:
            return {}

        self._ensure_connection()

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:

                cur.execute(
                    """
                    SELECT endereco_normalizado, lat, lon
                    FROM enderecos_cache
                    WHERE endereco_normalizado = ANY(%s)
                    """,
                    (cache_keys,),
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
    def buscar_cache_filtrado(
        self,
        cidade=None,
        uf=None,
        endereco=None,
        origem=None,
        atualizado_de=None,
        atualizado_ate=None,
        limit=50,
        offset=0,
        order_by="atualizado_em",
        order_dir="desc",
    ):

        self._ensure_connection()

        try:
            termos = normalize_for_cache(
                " ".join(
                    str(parte or "").strip()
                    for parte in [cidade, uf, endereco]
                    if str(parte or "").strip()
                )
            ).split()

            where_clauses = []
            params = []

            for termo in termos:
                where_clauses.append("COALESCE(endereco_normalizado, endereco) ILIKE %s")
                params.append(f"%{termo}%")

            if origem:
                where_clauses.append("origem = %s")
                params.append(origem)

            if atualizado_de:
                where_clauses.append("atualizado_em >= %s::timestamp")
                params.append(atualizado_de)

            if atualizado_ate:
                where_clauses.append("atualizado_em <= %s::timestamp")
                params.append(atualizado_ate)

            if not where_clauses:
                return {
                    "items": [],
                    "total": 0,
                    "limit": limit,
                    "offset": offset,
                }

            where_sql = " AND ".join(where_clauses)

            order_map = {
                "atualizado_em": "atualizado_em",
                "endereco": "endereco",
                "origem": "origem",
                "lat": "lat",
                "lon": "lon",
            }
            order_column = order_map.get(order_by, "atualizado_em")
            order_direction = "ASC" if str(order_dir).lower() == "asc" else "DESC"

            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:

                count_query = f"""
                SELECT COUNT(*) AS total
                FROM enderecos_cache
                WHERE {where_sql}
                """

                cur.execute(count_query, tuple(params))
                total_row = cur.fetchone() or {"total": 0}

                query = f"""
                SELECT id, endereco, endereco_normalizado, lat, lon, origem, atualizado_em
                FROM enderecos_cache
                WHERE {where_sql}
                ORDER BY {order_column} {order_direction}, id DESC
                LIMIT %s OFFSET %s
                """

                page_params = [*params, limit, offset]

                cur.execute(query, tuple(page_params))
                return {
                    "items": cur.fetchall(),
                    "total": int(total_row["total"] or 0),
                    "limit": limit,
                    "offset": offset,
                }

        except Exception as e:
            logger.warning(f"[CACHE_FILTRO][ERRO] {e}")
            return {
                "items": [],
                "total": 0,
                "limit": limit,
                "offset": offset,
            }

    def buscar_cache_por_id(self, cache_id: int):

        self._ensure_connection()

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, endereco, endereco_normalizado, lat, lon, origem, criado_em, atualizado_em
                    FROM enderecos_cache
                    WHERE id = %s
                    LIMIT 1
                    """,
                    (cache_id,),
                )
                return cur.fetchone()

        except Exception as e:
            logger.warning(f"[CACHE_BY_ID][ERRO] {e}")
            return None

    def buscar_cache_por_chave_normalizada(self, endereco_normalizado: str):

        if not endereco_normalizado:
            return None

        self._ensure_connection()

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, endereco, endereco_normalizado, lat, lon, origem, criado_em, atualizado_em
                    FROM enderecos_cache
                    WHERE endereco_normalizado = %s
                    LIMIT 1
                    """,
                    (endereco_normalizado,),
                )
                return cur.fetchone()

        except Exception as e:
            logger.warning(f"[CACHE_BY_KEY][ERRO] {e}")
            return None

    def buscar_cache_por_endereco(self, endereco: str):

        if not endereco:
            return None

        self._ensure_connection()

        try:
            with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(
                    """
                    SELECT id, endereco, endereco_normalizado, lat, lon, origem, criado_em, atualizado_em
                    FROM enderecos_cache
                    WHERE endereco = %s
                    LIMIT 1
                    """,
                    (endereco,),
                )
                return cur.fetchone()

        except Exception as e:
            logger.warning(f"[CACHE_BY_ENDERECO][ERRO] {e}")
            return None
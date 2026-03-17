#sales_router/src/geocoding_engine/infrastructure/database_reader.py

import os
import psycopg2
from psycopg2.extras import RealDictCursor
import unicodedata

def normalize(text):
    return unicodedata.normalize("NFKD", text).encode("ASCII", "ignore").decode("utf-8").upper()


class DatabaseReader:

    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
        )

    # ---------------------------------------------------------
    # CACHE GLOBAL INDIVIDUAL
    # ---------------------------------------------------------

    def buscar_cache(self, endereco):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT lat, lon
                FROM enderecos_cache
                WHERE endereco = %s
                LIMIT 1
                """,
                (endereco,),
            )

            row = cur.fetchone()

            if row:
                return row["lat"], row["lon"]

        return None

    # ---------------------------------------------------------
    # CACHE GLOBAL EM LOTE
    # ---------------------------------------------------------

    def buscar_cache_em_lote(self, enderecos):
        if not enderecos:
            return {}

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(
                """
                SELECT endereco, lat, lon
                FROM enderecos_cache
                WHERE endereco = ANY(%s)
                """,
                (list(enderecos),),
            )

            rows = cur.fetchall()

        return {
            r["endereco"]: (r["lat"], r["lon"])
            for r in rows
        }
   

    def buscar_cache_filtrado(self, cidade, uf, endereco=None, limit=50):

        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:

            termos = normalize(f"{cidade} {uf} {endereco or ''}").split()

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
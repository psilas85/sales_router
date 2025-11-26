#sales_router/src/pdv_preprocessing/cep_area_geocoding/infrastructure/database_reader.py

# ============================================================
# 📦 src/pdv_preprocessing/cep_area_geocoding/infrastructure/database_reader.py
# ============================================================

from pdv_preprocessing.infrastructure.database_reader import POOL
from loguru import logger


class DatabaseReader:

    # --------------------------------------------------------
    def carregar_marketplace_ceps(self, tenant_id, input_id):
        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT cep, bairro, cidade, uf
                    FROM marketplace_cep
                    WHERE tenant_id = %s AND input_id = %s
                """, (tenant_id, input_id))
                return cur.fetchall()
        finally:
            POOL.putconn(conn)

    # --------------------------------------------------------
    def buscar_cep_bairro_cache(self, tenant_id, cep):
        cep = str(cep).strip()

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT lat, lon, origem
                    FROM cep_bairro_cache
                    WHERE tenant_id = %s AND cep = %s
                """, (tenant_id, cep))
                row = cur.fetchone()
                return row  # (lat, lon, origem)
        finally:
            POOL.putconn(conn)

    # --------------------------------------------------------
    # 🔥 Busca múltiplos CEPs no cache — otimizado
    # --------------------------------------------------------
    def buscar_lista_cache_bairro(self, tenant_id, lista_ceps):
        """
        Retorna dict:
            {
                "04844270": (lat, lon, origem),
                "08140380": (lat, lon, "google"),
                ...
            }
        """
        if not lista_ceps:
            return {}

        # Normalizar lista
        lista = [
            str(c).strip()
            for c in lista_ceps
            if c is not None and str(c).strip() != "" and str(c).lower() != "nan"
        ]

        if not lista:
            return {}

        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT cep, lat, lon, origem
                    FROM cep_bairro_cache
                    WHERE tenant_id = %s AND cep = ANY(%s)
                """, (tenant_id, lista))

                rows = cur.fetchall()

                cache = {}
                for cep, lat, lon, origem in rows:
                    cache[cep] = (lat, lon, origem)

                # logger.debug(f"🗄️ Cache DB retornou {len(cache)} registros")

                return cache

        finally:
            POOL.putconn(conn)

    # --------------------------------------------------------
    def buscar_ultimo_input_id(self, tenant_id):
        """
        Retorna último input_id da tabela marketplace_cep.
        """
        conn = POOL.getconn()
        try:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT input_id
                    FROM marketplace_cep
                    WHERE tenant_id = %s
                    ORDER BY criado_em DESC
                    LIMIT 1
                """, (tenant_id,))
                row = cur.fetchone()
                return row[0] if row else None
        finally:
            POOL.putconn(conn)

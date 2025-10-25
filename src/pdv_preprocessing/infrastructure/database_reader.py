#sales_router/src/pdv_preprocessing/infrastructure/database_reader.py

import logging
import time
import pandas as pd
from psycopg2.extras import RealDictCursor


class DatabaseReader:
    """
    Responsável por leituras no banco de dados PostgreSQL
    relacionadas a endereços e PDVs existentes.
    """

    def __init__(self, conn):
        self.conn = conn

    # ==========================================================
    # 🔍 Consulta cache de coordenadas
    # ==========================================================
    def buscar_localizacao(self, endereco: str):
        """
        Busca coordenadas (lat, lon) no cache persistente
        da tabela enderecos_cache.
        """
        if not endereco:
            return None
        try:
            inicio = time.time()
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT lat, lon
                FROM enderecos_cache
                WHERE endereco = %s
                LIMIT 1;
            """, (endereco.strip().lower(),))
            row = cur.fetchone()
            dur = time.time() - inicio
            cur.close()

            if row and row["lat"] is not None and row["lon"] is not None:
                logging.info(f"🗄️ [CACHE_DB] ({dur:.2f}s) {endereco} → ({row['lat']}, {row['lon']})")
                return (row["lat"], row["lon"])
            logging.debug(f"📭 [CACHE_DB] Sem resultado para {endereco} ({dur:.2f}s)")
            return None

        except Exception as e:
            logging.warning(f"⚠️ [CACHE_DB] Falha ao buscar cache no banco: {e}")
            return None

    # ==========================================================
    # 🧠 Consulta PDV existente por tenant e CNPJ
    # ==========================================================
    def buscar_pdv_por_cnpj(self, tenant_id: int, cnpj: str):
        """
        Verifica se já existe um PDV cadastrado para o mesmo
        tenant_id e CNPJ (para validação ou atualização futura).
        """
        try:
            inicio = time.time()
            cur = self.conn.cursor(cursor_factory=RealDictCursor)
            cur.execute("""
                SELECT id, cnpj, cidade, uf, pdv_lat, pdv_lon
                FROM pdvs
                WHERE tenant_id = %s AND cnpj = %s
                LIMIT 1;
            """, (tenant_id, cnpj))
            row = cur.fetchone()
            dur = time.time() - inicio
            cur.close()

            if row:
                logging.debug(f"📋 [PDV_DB] ({dur:.2f}s) Encontrado CNPJ {cnpj}")
            else:
                logging.debug(f"📋 [PDV_DB] ({dur:.2f}s) Não encontrado CNPJ {cnpj}")
            return row

        except Exception as e:
            logging.warning(f"⚠️ [PDV_DB] Erro ao buscar PDV existente ({cnpj}): {e}")
            return None

    # ==========================================================
    # 📋 Carrega todos os PDVs de um tenant (opcional)
    # ==========================================================
    def listar_pdvs_por_tenant(self, tenant_id: int) -> pd.DataFrame:
        """
        Retorna DataFrame com todos os PDVs de um tenant.
        Pode ser usado futuramente para clusterização ou dashboards.
        """
        try:
            inicio = time.time()
            query = """
                SELECT *
                FROM pdvs
                WHERE tenant_id = %s
                ORDER BY cidade, bairro;
            """
            df = pd.read_sql_query(query, self.conn, params=(tenant_id,))
            dur = time.time() - inicio
            logging.info(f"📊 [PDV_DB] {len(df)} PDVs carregados do tenant {tenant_id} ({dur:.2f}s)")
            return df
        except Exception as e:
            logging.warning(f"⚠️ [PDV_DB] Erro ao listar PDVs do tenant {tenant_id}: {e}")
            return pd.DataFrame()

    # ==========================================================
    # 🧾 Busca CNPJs existentes (respeitando input_id)
    # ==========================================================
    def buscar_cnpjs_existentes(self, tenant_id: int, input_id: str | None = None) -> list[str]:
        """
        Retorna os CNPJs já cadastrados para o tenant.
        Se input_id for informado, filtra apenas por aquele input_id.
        Isso evita bloquear CNPJs de outros processamentos.
        """
        try:
            cur = self.conn.cursor()

            if input_id:
                cur.execute(
                    "SELECT cnpj FROM pdvs WHERE tenant_id = %s AND input_id = %s;",
                    (tenant_id, input_id),
                )
            else:
                cur.execute(
                    "SELECT cnpj FROM pdvs WHERE tenant_id = %s;",
                    (tenant_id,),
                )

            cnpjs = [row[0] for row in cur.fetchall()]
            cur.close()
            logging.debug(f"📋 [PDV_DB] {len(cnpjs)} CNPJs retornados para tenant={tenant_id}, input_id={input_id}")
            return cnpjs

        except Exception as e:
            logging.warning(f"⚠️ [PDV_DB] Erro ao buscar CNPJs existentes (tenant={tenant_id}, input_id={input_id}): {e}")
            return []


    def buscar_enderecos_cache(self, enderecos: list[str]) -> dict[str, tuple[float, float]]:
        """
        Retorna um dicionário {endereco: (lat, lon)} com endereços já presentes no cache.
        """
        if not enderecos:
            return {}
        cur = self.conn.cursor()
        cur.execute(
            """
            SELECT endereco, lat, lon
            FROM enderecos_cache
            WHERE endereco = ANY(%s)
            """,
            (enderecos,),
        )
        resultados = {row[0].strip().lower(): (row[1], row[2]) for row in cur.fetchall()}
        cur.close()
        return resultados
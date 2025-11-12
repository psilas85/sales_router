#sales_router/src/pdv_preprocessing/infrastructure/database_reader.py

import logging
import time
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import closing
from typing import Optional, Dict, List, Tuple, Any
from functools import wraps

# ============================================================
# 🔁 Decorator de retry com backoff exponencial
# ============================================================
def retry_on_failure(max_retries=3, delay=1.0, backoff=2.0):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            tentativa = 0
            while tentativa < max_retries:
                try:
                    return func(*args, **kwargs)
                except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                    tentativa += 1
                    logging.warning(f"⚠️ Erro de conexão na tentativa {tentativa}/{max_retries}: {e}")
                    time.sleep(delay * (backoff ** (tentativa - 1)))
                except Exception as e:
                    logging.error(f"❌ Erro inesperado em {func.__name__}: {e}", exc_info=True)
                    break
            logging.error(f"🚨 Falha após {max_retries} tentativas em {func.__name__}")
            return None
        return wrapper
    return decorator


class DatabaseReader:
    """
    Responsável por leituras no banco de dados PostgreSQL
    relacionadas a endereços e PDVs existentes.
    """

    def __init__(self, conn):
        self.conn = conn

    # ============================================================
    # 🔍 Consulta cache de coordenadas
    # ============================================================
    @retry_on_failure()
    def buscar_localizacao(self, endereco: str) -> Optional[Tuple[float, float]]:
        """Busca coordenadas (lat, lon) no cache persistente da tabela enderecos_cache."""
        if not endereco:
            return None
        inicio = time.time()
        try:
            with closing(self.conn.cursor(cursor_factory=RealDictCursor)) as cur:
                cur.execute(
                    "SELECT lat, lon FROM enderecos_cache WHERE endereco = %s LIMIT 1;",
                    (endereco.strip().lower(),),
                )
                row = cur.fetchone()
            dur = time.time() - inicio
            if row and row["lat"] is not None and row["lon"] is not None:
                logging.info(f"🗄️ [CACHE_DB] ({dur:.2f}s) {endereco} → ({row['lat']}, {row['lon']})")
                return (row["lat"], row["lon"])
            logging.debug(f"📭 [CACHE_DB] ({dur:.2f}s) Sem resultado para {endereco}")
            return None
        except Exception as e:
            logging.warning(f"⚠️ [CACHE_DB] Falha ao buscar cache: {e}")
            return None

    # ============================================================
    # 🧠 Consulta PDV existente por tenant e CNPJ
    # ============================================================
    @retry_on_failure()
    def buscar_pdv_por_cnpj(self, tenant_id: int, cnpj: str) -> Optional[Dict[str, Any]]:
        """Verifica se já existe um PDV cadastrado para o mesmo tenant_id e CNPJ."""
        inicio = time.time()
        try:
            with closing(self.conn.cursor(cursor_factory=RealDictCursor)) as cur:
                cur.execute(
                    """
                    SELECT id, cnpj, cidade, uf, pdv_lat, pdv_lon
                    FROM pdvs
                    WHERE tenant_id = %s AND cnpj = %s
                    LIMIT 1;
                    """,
                    (tenant_id, cnpj),
                )
                row = cur.fetchone()
            dur = time.time() - inicio
            msg = "Encontrado" if row else "Não encontrado"
            logging.debug(f"📋 [PDV_DB] ({dur:.2f}s) {msg} CNPJ {cnpj}")
            return row
        except Exception as e:
            logging.warning(f"⚠️ [PDV_DB] Erro ao buscar PDV existente ({cnpj}): {e}")
            return None

    # ============================================================
    # 📋 Carrega todos os PDVs de um tenant
    # ============================================================
    @retry_on_failure()
    def listar_pdvs_por_tenant(self, tenant_id: int) -> pd.DataFrame:
        """Retorna DataFrame com todos os PDVs de um tenant (para dashboards e clusterização)."""
        inicio = time.time()
        try:
            query = """
                SELECT *
                FROM pdvs
                WHERE tenant_id = %s
                ORDER BY cidade, bairro;
            """
            df = pd.read_sql_query(query, self.conn, params=(tenant_id,))

            # 🧹 Sanitização total (remove NaN, inf, -inf)
            df = df.replace([float("inf"), float("-inf")], pd.NA)
            df = df.where(pd.notnull(df), None)

            dur = time.time() - inicio
            logging.info(f"📊 [PDV_DB] {len(df)} PDVs carregados (tenant={tenant_id}, {dur:.2f}s)")
            return df

        except Exception as e:
            logging.warning(f"⚠️ [PDV_DB] Erro ao listar PDVs (tenant={tenant_id}): {e}")
            return pd.DataFrame()

        # ============================================================
        # 🧾 Busca CNPJs existentes (respeitando input_id)
        # ============================================================
        @retry_on_failure()
        def buscar_cnpjs_existentes(self, tenant_id: int, input_id: Optional[str] = None) -> List[str]:
            """Retorna os CNPJs já cadastrados para o tenant (filtrando por input_id se informado)."""
            query = (
                "SELECT cnpj FROM pdvs WHERE tenant_id = %s AND input_id = %s;"
                if input_id
                else "SELECT cnpj FROM pdvs WHERE tenant_id = %s;"
            )
            params = (tenant_id, input_id) if input_id else (tenant_id,)
            try:
                with closing(self.conn.cursor()) as cur:
                    cur.execute(query, params)
                    cnpjs = [row[0] for row in cur.fetchall()]
                logging.debug(f"📋 [PDV_DB] {len(cnpjs)} CNPJs retornados (tenant={tenant_id}, input_id={input_id})")
                return cnpjs
            except Exception as e:
                logging.warning(f"⚠️ [PDV_DB] Erro ao buscar CNPJs existentes (tenant={tenant_id}): {e}")
                return []

    # ============================================================
    # 🗺️ Busca múltiplos endereços no cache
    # ============================================================
    @retry_on_failure()
    def buscar_enderecos_cache(self, enderecos: List[str]) -> Dict[str, Tuple[float, float]]:
        """Retorna dicionário {endereco: (lat, lon)} com endereços já presentes no cache."""
        if not enderecos:
            return {}
        try:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    """
                    SELECT endereco, lat, lon
                    FROM enderecos_cache
                    WHERE endereco = ANY(%s);
                    """,
                    (enderecos,),
                )
                resultados = {
                    row[0].strip().lower(): (row[1], row[2]) for row in cur.fetchall()
                }
            return resultados
        except Exception as e:
            logging.warning(f"⚠️ [CACHE_DB] Erro ao buscar múltiplos endereços: {e}")
            return {}

    # ============================================================
    # 📦 Busca localizações por CEP (enderecos_cache)
    # ============================================================
    @retry_on_failure()
    def buscar_localizacoes_por_ceps(self, lista_ceps: List[str]) -> List[Dict[str, Any]]:
        """Retorna lat/lon de todos os CEPs informados que já existem no cache do banco."""
        if not lista_ceps:
            return []
        try:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    """
                    SELECT endereco, lat, lon
                    FROM enderecos_cache
                    WHERE endereco = ANY(%s);
                    """,
                    (lista_ceps,),
                )
                colunas = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(colunas, r)) for r in rows]
        except Exception as e:
            logging.error(f"❌ Erro ao buscar CEPs no cache: {e}", exc_info=True)
            return []

    # ============================================================
    # 🧭 Busca coordenadas no cache MKP (individual e batch)
    # ============================================================
    @retry_on_failure()
    def buscar_localizacao_mkp(self, cep: str) -> Optional[Tuple[float, float]]:
        """Retorna (lat, lon) para o CEP informado no cache de marketplace."""
        if not cep:
            return None
        try:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    "SELECT lat, lon FROM mkp_enderecos_cache WHERE cep = %s LIMIT 1;",
                    (str(cep).zfill(8),),
                )
                row = cur.fetchone()
                return (row[0], row[1]) if row and row[0] and row[1] else None
        except Exception as e:
            logging.warning(f"⚠️ [MKP_CACHE] Falha ao buscar CEP {cep}: {e}")
            return None

    @retry_on_failure()
    def buscar_localizacoes_mkp_por_ceps(self, lista_ceps: List[str]) -> List[Dict[str, Any]]:
        """Retorna todos os CEPs que já existem no cache MKP."""
        if not lista_ceps:
            return []
        try:
            with closing(self.conn.cursor()) as cur:
                cur.execute(
                    """
                    SELECT cep, lat, lon
                    FROM mkp_enderecos_cache
                    WHERE cep = ANY(%s);
                    """,
                    (lista_ceps,),
                )
                colunas = [desc[0] for desc in cur.description]
                rows = cur.fetchall()
                return [dict(zip(colunas, r)) for r in rows]
        except Exception as e:
            logging.error(f"❌ Erro ao buscar CEPs no cache MKP: {e}", exc_info=True)
            return []

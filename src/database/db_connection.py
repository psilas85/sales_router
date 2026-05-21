#sales_router/src/database/db_connection.py

import os
import time
import psycopg2
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from contextlib import contextmanager
from loguru import logger


# =====================================================
# ⚙️ Configuração do banco
# =====================================================
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME", os.getenv("POSTGRES_DB", "sales_routing_db")),
    "user": os.getenv("DB_USER", os.getenv("POSTGRES_USER", "postgres")),
    "password": os.getenv("DB_PASSWORD", os.getenv("POSTGRES_PASSWORD", "postgres")),
    "host": os.getenv("DB_HOST", os.getenv("POSTGRES_HOST", "sales_router_db")),
    "port": os.getenv("DB_PORT", os.getenv("POSTGRES_PORT", "5432")),
    "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "10")),
    "application_name": os.getenv("DB_APP_NAME", "sales_router"),
}


# =====================================================
# 🔄 Retentativas automáticas com backoff exponencial
# =====================================================
def get_connection(retries: int = 5, delay: int = 2, backoff: float = 1.5):
    """
    Cria e retorna uma conexão com o PostgreSQL.
    Retenta automaticamente em caso de falha temporária.
    """
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            conn.autocommit = False
            logger.debug(f"✅ Conexão PostgreSQL estabelecida (tentativa {attempt})")
            return conn
        except OperationalError as e:
            wait = delay * (backoff ** (attempt - 1))
            logger.warning(f"⚠️ Erro de conexão (tentativa {attempt}/{retries}): {e} — aguardando {wait:.1f}s")
            time.sleep(wait)
        except Exception as e:
            logger.error(f"❌ Erro inesperado ao conectar: {e}", exc_info=True)
            time.sleep(delay)

    raise ConnectionError("❌ Falha ao conectar ao banco após múltiplas tentativas.")


# =====================================================
# 🧱 Context Manager seguro (rollback e fechamento)
# =====================================================
_SCHEMAS_PERMITIDOS = {"public", "operacional"}


@contextmanager
def get_connection_context(retries: int = 3, schema: str | None = None):
    """
    Context manager seguro para uso de conexões PostgreSQL.
    Fecha e faz rollback automaticamente em caso de erro.

    `schema`: quando informado (e diferente de 'public'), aplica
    `SET search_path TO <schema>, public` na conexão — usado pela Execução
    Operacional para resolver as tabelas no schema `operacional`, com
    fallback em `public` para as tabelas-globais. Default (None) não altera
    o search_path — comportamento idêntico ao original.

    Exemplo:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
    """
    conn = None
    try:
        conn = get_connection(retries=retries)
        if schema and schema != "public":
            if schema not in _SCHEMAS_PERMITIDOS:
                raise ValueError(f"schema inválido: {schema!r}")
            with conn.cursor() as cur:
                cur.execute(f"SET search_path TO {schema}, public")
        yield conn
        conn.commit()
    except (OperationalError, InterfaceError) as e:
        if conn:
            conn.rollback()
        logger.error(f"💥 Erro operacional na conexão: {e}")
        raise
    except DatabaseError as e:
        if conn:
            conn.rollback()
        logger.error(f"❌ Erro de banco de dados: {e}")
        raise
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"⚠️ Exceção não tratada: {e}", exc_info=True)
        raise
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("🔌 Conexão PostgreSQL fechada com sucesso.")
            except Exception as e:
                logger.warning(f"⚠️ Falha ao fechar conexão: {e}")


# =====================================================
# 🔍 Verificação rápida (saúde do banco)
# =====================================================
def test_db_connection() -> bool:
    """
    Testa a conexão com o banco de dados e retorna True/False.
    Útil para inicialização de containers e healthchecks.
    """
    try:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT NOW();")
                result = cur.fetchone()
                logger.success(f"✅ Banco conectado. Hora atual: {result[0]}")
        return True
    except Exception as e:
        logger.error(f"❌ Falha ao testar conexão com o banco: {e}")
        return False

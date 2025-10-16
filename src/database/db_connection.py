#sales_router/src/database/db_connection.py

import psycopg2
from psycopg2 import OperationalError, InterfaceError, DatabaseError
from contextlib import contextmanager
import os
import time
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
    "connect_timeout": int(os.getenv("DB_CONNECT_TIMEOUT", "10")),  # ⏰ Timeout seguro
    "application_name": os.getenv("DB_APP_NAME", "sales_router"),
}


# =====================================================
# 🔄 Função com retentativas automáticas
# =====================================================
def get_connection(retries: int = 5, delay: int = 3):
    """
    Cria e retorna uma conexão com o banco PostgreSQL.
    Retenta automaticamente em caso de erro temporário ou inicialização lenta do banco.
    """
    for attempt in range(1, retries + 1):
        try:
            conn = psycopg2.connect(**DB_PARAMS)
            logger.debug(f"✅ Conexão PostgreSQL estabelecida (tentativa {attempt})")
            return conn
        except OperationalError as e:
            logger.warning(f"⚠️ Erro de conexão (tentativa {attempt}/{retries}): {e}")
            time.sleep(delay)
        except Exception as e:
            logger.error(f"❌ Erro inesperado ao conectar ao banco: {e}")
            time.sleep(delay)

    raise Exception("❌ Não foi possível conectar ao banco após múltiplas tentativas.")


# =====================================================
# 🧱 Context Manager seguro (fecha e faz rollback automático)
# =====================================================
@contextmanager
def get_connection_context():
    """
    Context manager para uso seguro da conexão com o PostgreSQL.
    Fecha automaticamente mesmo em caso de erro.
    Exemplo:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1;")
    """
    conn = None
    try:
        conn = get_connection()
        yield conn
        conn.commit()
    except (OperationalError, InterfaceError) as e:
        logger.error(f"💥 Erro operacional na conexão: {e}")
        if conn:
            conn.rollback()
        raise
    except DatabaseError as e:
        logger.error(f"❌ Erro de banco de dados: {e}")
        if conn:
            conn.rollback()
        raise
    except Exception as e:
        logger.error(f"⚠️ Exceção não tratada durante operação: {e}")
        if conn:
            conn.rollback()
        raise
    finally:
        if conn:
            try:
                conn.close()
                logger.debug("🔌 Conexão PostgreSQL fechada com sucesso.")
            except Exception as e:
                logger.warning(f"⚠️ Falha ao fechar conexão: {e}")


# =====================================================
# 🔍 Função de verificação rápida de conexão
# =====================================================
def test_db_connection():
    """
    Testa a conexão com o banco e retorna True/False.
    Útil para verificações em inicialização de containers.
    """
    try:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT NOW();")
                result = cur.fetchone()
                logger.success(f"✅ Banco conectado com sucesso. Hora atual: {result[0]}")
        return True
    except Exception as e:
        logger.error(f"❌ Falha ao testar conexão com o banco: {e}")
        return False

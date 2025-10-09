#sales_router/src/database/db_connection.py

import psycopg2
from psycopg2 import OperationalError
import os
import time


def get_connection(retries: int = 5, delay: int = 3):
    """
    Cria e retorna uma conexão com o banco PostgreSQL (container sales_router_db).
    Retenta automaticamente caso o banco ainda esteja inicializando.
    """
    db_name = os.getenv("POSTGRES_DB", "sales_routing_db")
    db_user = os.getenv("POSTGRES_USER", "postgres")
    db_password = os.getenv("POSTGRES_PASSWORD", "postgres")
    db_host = os.getenv("POSTGRES_HOST", "sales_router_db")
    db_port = os.getenv("POSTGRES_PORT", "5432")

    for attempt in range(retries):
        try:
            conn = psycopg2.connect(
                dbname=db_name,
                user=db_user,
                password=db_password,
                host=db_host,
                port=db_port
            )
            return conn
        except OperationalError as e:
            print(f"⚠️ Erro ao conectar ao banco (tentativa {attempt + 1}/{retries}): {e}")
            time.sleep(delay)
    raise Exception("❌ Não foi possível conectar ao banco após várias tentativas.")

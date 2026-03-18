#sales_router/src/routing_engine/infrastructure/database_connection.py

import os
import psycopg2
from loguru import logger
from dotenv import load_dotenv

load_dotenv()


def get_db_connection():
    """
    Cria conexão com PostgreSQL
    """

    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"),
        )

        logger.info("[DB] conexão estabelecida com sucesso")

        return conn

    except Exception as e:
        logger.error(f"[DB] erro ao conectar: {e}")
        raise


def fechar_conexao(conn):
    """
    Fecha conexão com banco
    """

    try:
        if conn:
            conn.close()
            logger.info("[DB] conexão fechada")
    except Exception as e:
        logger.warning(f"[DB] erro ao fechar conexão: {e}")
#sales_router/src/authentication/infrastructure/db_connection.py

import psycopg2
import os

def get_connection():
    return psycopg2.connect(
        dbname="sales_routing_db",
        user="postgres",
        password="postgres",
        host="sales_router_db",
        port="5432"
    )

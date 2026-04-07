#sales_router/src/routing_engine/services/consultor_service.py

from routing_engine.infrastructure.database_connection import get_db_connection


class ConsultorService:

    def __init__(self, tenant_id: int):
        self.conn = get_db_connection()
        self.tenant_id = tenant_id

    def get_base(self, consultor: str):

        query = """
            SELECT lat, lon
            FROM consultores
            WHERE tenant_id = %s
              AND consultor = %s
        """

        with self.conn.cursor() as cur:
            cur.execute(query, (self.tenant_id, consultor))
            row = cur.fetchone()

        if not row:
            raise ValueError(f"Consultor não encontrado: {consultor}")

        lat, lon = row

        if lat is None or lon is None:
            raise ValueError(f"Consultor sem coordenadas: {consultor}")

        return float(lat), float(lon)

    def get_all_consultores(self):

        query = """
            SELECT DISTINCT UPPER(consultor)
            FROM consultores
            WHERE tenant_id = %s
        """

        with self.conn.cursor() as cur:
            cur.execute(query, (self.tenant_id,))
            rows = cur.fetchall()

        # retorna lista simples
        return [r[0] for r in rows if r[0]]
# src/sales_clusterization/infrastructure/persistence/database_reader.py

from typing import List, Optional
from src.database.db_connection import get_connection
from src.sales_clusterization.domain.entities import PDV


def carregar_pdvs(tenant_id: int, uf: Optional[str] = None, cidade: Optional[str] = None) -> List[PDV]:
    """
    Lê os PDVs válidos do tenant informado.
    Filtros opcionais de UF e cidade.
    Retorna lista de objetos PDV com latitude e longitude válidas.
    A comparação é case-insensitive (usa UPPER()).
    """
    query = """
        SELECT id, cnpj, bairro, cidade, uf, pdv_lat, pdv_lon
        FROM pdvs
        WHERE tenant_id = %s
          AND pdv_lat IS NOT NULL
          AND pdv_lon IS NOT NULL
          AND (%s IS NULL OR UPPER(uf) = UPPER(%s))
          AND (%s IS NULL OR UPPER(cidade) = UPPER(%s));
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (tenant_id, uf, uf, cidade, cidade))
            rows = cur.fetchall()

    return [
        PDV(
            id=row[0],
            cnpj=row[1],
            nome=None,
            bairro=row[2],
            cidade=row[3],
            uf=row[4],
            lat=row[5],
            lon=row[6],
        )
        for row in rows
    ]

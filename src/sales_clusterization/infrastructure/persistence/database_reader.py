#sales_clusterization/infrastructure/persistence/database_reader.py

from typing import List, Optional
from src.database.db_connection import get_connection
from src.sales_clusterization.domain.entities import PDV


def carregar_pdvs(uf: Optional[str] = None, cidade: Optional[str] = None) -> List[PDV]:
    """
    Lê os PDVs da tabela principal conforme filtros de UF e/ou cidade.
    Retorna uma lista de objetos PDV com latitude e longitude válidas.
    """
    query = """
        SELECT id, cnpj, bairro, cidade, uf, pdv_lat, pdv_lon
        FROM pdvs
        WHERE pdv_lat IS NOT NULL
          AND pdv_lon IS NOT NULL
          AND (%s IS NULL OR uf = %s)
          AND (%s IS NULL OR cidade = %s);
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (uf, uf, cidade, cidade))
            rows = cur.fetchall()

    return [
        PDV(
            id=row[0],
            cnpj=row[1],
            nome=None,  # compatibilidade de estrutura
            bairro=row[2],
            cidade=row[3],
            uf=row[4],
            lat=row[5],
            lon=row[6],
        )
        for row in rows
    ]

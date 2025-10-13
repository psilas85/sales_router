# src/sales_routing/infrastructure/database_reader.py

import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData
import os


class SalesRoutingDatabaseReader:
    """
    Leitor de dados de clusterização para o módulo Sales Routing.
    Busca dados consolidados no banco sales_routing_db:
      - Último run concluído
      - Setores (clusters)
      - PDVs associados a cada cluster
    """

    def __init__(self):
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "sales_routing_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASS", "postgres"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
        )

    # -------------------------------------------------------------------------
    def get_last_run(self) -> Optional[Dict[str, Any]]:
        """Retorna o último run concluído (status='done')."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, uf, cidade, algo, k_final, params
                FROM cluster_run
                WHERE status = 'done'
                ORDER BY id DESC
                LIMIT 1;
            """)
            row = cur.fetchone()
            return dict(row) if row else None

    # -------------------------------------------------------------------------
    def get_clusters(self, run_id: int) -> List[ClusterData]:
        """Busca os clusters (setores) de um run específico."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    id AS cluster_id,
                    run_id,
                    cluster_label,
                    centro_lat,
                    centro_lon,
                    n_pdvs,
                    metrics
                FROM cluster_setor
                WHERE run_id = %s
                ORDER BY cluster_label;
            """, (run_id,))
            rows = cur.fetchall()
            return [
                ClusterData(
                    run_id=row["run_id"],
                    cluster_id=row["cluster_id"],
                    cluster_label=row["cluster_label"],
                    centro_lat=row["centro_lat"],
                    centro_lon=row["centro_lon"],
                    n_pdvs=row["n_pdvs"],
                    metrics=row["metrics"]
                )
                for row in rows
            ]

    # -------------------------------------------------------------------------
    def get_pdvs(self, run_id: int) -> List[PDVData]:
        """Busca os PDVs mapeados de um run específico."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    p.run_id,
                    p.cluster_id,
                    p.pdv_id,
                    p.lat,
                    p.lon,
                    p.cidade,
                    p.uf
                FROM cluster_setor_pdv p
                WHERE p.run_id = %s
                ORDER BY p.pdv_id;
            """, (run_id,))
            rows = cur.fetchall()
            return [
                PDVData(
                    run_id=row["run_id"],
                    cluster_id=row["cluster_id"],
                    pdv_id=row["pdv_id"],
                    lat=row["lat"],
                    lon=row["lon"],
                    cidade=row["cidade"],
                    uf=row["uf"]
                )
                for row in rows
            ]

    def get_last_run_by_location(self, uf: str, cidade: str):
        """Retorna o último run concluído filtrado por UF e cidade."""
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, uf, cidade, algo, k_final, params
                FROM cluster_run
                WHERE status = 'done' AND uf = %s AND cidade = %s
                ORDER BY id DESC
                LIMIT 1;
            """, (uf, cidade))
            row = cur.fetchone()
            return dict(row) if row else None


    # -------------------------------------------------------------------------
    def close(self):
        """Fecha a conexão com o banco."""
        if self.conn:
            self.conn.close()

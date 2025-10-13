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
    # Dentro de SalesRoutingDatabaseReader

    def list_snapshots(self, tenant_id, uf=None, cidade=None):
        cur = self.conn.cursor()
        query = """
            SELECT id, nome, descricao, criado_em, tags, uf, cidade
            FROM sales_routing_snapshot
            WHERE tenant_id = %s
        """
        params = [tenant_id]

        if uf:
            query += " AND uf = %s"
            params.append(uf)
        if cidade:
            query += " AND cidade = %s"
            params.append(cidade)

        query += " ORDER BY criado_em DESC;"
        cur.execute(query, tuple(params))
        rows = cur.fetchall()
        cur.close()

        return [
            {
                "id": r[0],
                "nome": r[1],
                "descricao": r[2],
                "criado_em": r[3],
                "tags": r[4],
                "uf": r[5],
                "cidade": r[6],
            }
            for r in rows
        ]


    def get_snapshot_by_name(self, tenant_id, nome):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, nome, descricao, uf, cidade
            FROM sales_routing_snapshot
            WHERE tenant_id = %s AND nome = %s
            ORDER BY criado_em DESC
            LIMIT 1;
        """, (tenant_id, nome))
        row = cur.fetchone()
        cur.close()
        if not row:
            return None
        return {
            "id": row[0],
            "nome": row[1],
            "descricao": row[2],
            "uf": row[3],
            "cidade": row[4],
        }


    def get_snapshot_subclusters(self, snapshot_id):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT cluster_id, subcluster_seq, k_final, tempo_total_min, dist_total_km, n_pdvs
            FROM sales_routing_snapshot_subcluster
            WHERE snapshot_id = %s;
        """, (snapshot_id,))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "cluster_id": r[0],
                "subcluster_seq": r[1],
                "k_final": r[2],
                "tempo_total_min": r[3],
                "dist_total_km": r[4],
                "n_pdvs": r[5],
            }
            for r in rows
        ]


    def get_snapshot_pdvs(self, snapshot_id):
        cur = self.conn.cursor()
        cur.execute("""
            SELECT cluster_id, subcluster_seq, pdv_id, sequencia_ordem
            FROM sales_routing_snapshot_pdv
            WHERE snapshot_id = %s;
        """, (snapshot_id,))
        rows = cur.fetchall()
        cur.close()
        return [
            {
                "cluster_id": r[0],
                "subcluster_seq": r[1],
                "pdv_id": r[2],
                "sequencia_ordem": r[3],
            }
            for r in rows
        ]


    # -------------------------------------------------------------------------
    def close(self):
        """Fecha a conexão com o banco."""
        if self.conn:
            self.conn.close()

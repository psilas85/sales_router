# src/sales_routing/infrastructure/database_reader.py

import os
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Any, Optional
from loguru import logger
from src.database.db_connection import get_connection_context
from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData


class SalesRoutingDatabaseReader:
    """
    Classe de leitura de dados para o módulo Sales Routing.
    Todas as consultas usam context manager para garantir
    fechamento automático de conexões (compatível com PgBouncer).
    """

    # =========================================================
    # 1️⃣ Último run concluído
    # =========================================================
    def get_last_run(self) -> Optional[Dict[str, Any]]:
        """Retorna o último run concluído (status='done')."""
        with get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, uf, cidade, algo, k_final, params
                    FROM cluster_run
                    WHERE status = 'done'
                    ORDER BY id DESC
                    LIMIT 1;
                """)
                row = cur.fetchone()
                return dict(row) if row else None

    # =========================================================
    # 2️⃣ Busca clusters (setores)
    # =========================================================
    def get_clusters(self, run_id: int) -> List[ClusterData]:
        """Busca os clusters (setores) de um run específico."""
        with get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
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

    # =========================================================
    # 3️⃣ Busca PDVs
    # =========================================================
    def get_pdvs(self, run_id: int) -> List[PDVData]:
        """Busca os PDVs mapeados de um run específico."""
        with get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
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

    # =========================================================
    # 4️⃣ Último run por localização
    # =========================================================
    def get_last_run_by_location(self, uf: str, cidade: Optional[str]):
        """Retorna o último run concluído filtrado por UF e cidade."""
        with get_connection_context() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT id, uf, cidade, algo, k_final, params
                    FROM cluster_run
                    WHERE status = 'done'
                      AND uf = %s
                      AND (cidade = %s OR (%s IS NULL AND cidade IS NULL))
                    ORDER BY id DESC
                    LIMIT 1;
                """, (uf, cidade, cidade))
                row = cur.fetchone()
                if not row:
                    logger.warning(f"⚠️ Nenhum run encontrado para UF={uf}, cidade={cidade}")
                    return None

                logger.info(
                    f"📦 Último run encontrado: id={row['id']} | UF={row['uf']} | cidade={row['cidade']} | algo={row['algo']}"
                )
                return dict(row)

    # =========================================================
    # 5️⃣ Lista snapshots
    # =========================================================
    def list_snapshots(self, tenant_id, uf=None, cidade=None):
        """Lista snapshots do tenant, com filtros opcionais por UF e cidade."""
        with get_connection_context() as conn:
            with conn.cursor() as cur:
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

    # =========================================================
    # 6️⃣ Busca snapshot por nome
    # =========================================================
    def get_snapshot_by_name(self, tenant_id, nome):
        """Busca um snapshot específico pelo nome."""
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, nome, descricao, uf, cidade
                    FROM sales_routing_snapshot
                    WHERE tenant_id = %s AND nome = %s
                    ORDER BY criado_em DESC
                    LIMIT 1;
                """, (tenant_id, nome))
                row = cur.fetchone()
                if not row:
                    return None
                return {
                    "id": row[0],
                    "nome": row[1],
                    "descricao": row[2],
                    "uf": row[3],
                    "cidade": row[4],
                }

    # =========================================================
    # 7️⃣ Subclusters e PDVs de snapshot
    # =========================================================
    def get_snapshot_subclusters(self, snapshot_id):
        """Retorna subclusters de um snapshot específico."""
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT cluster_id, subcluster_seq, k_final, tempo_total_min,
                           dist_total_km, n_pdvs
                    FROM sales_routing_snapshot_subcluster
                    WHERE snapshot_id = %s;
                """, (snapshot_id,))
                rows = cur.fetchall()
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
        """Retorna PDVs de um snapshot específico."""
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT cluster_id, subcluster_seq, pdv_id, sequencia_ordem
                    FROM sales_routing_snapshot_pdv
                    WHERE snapshot_id = %s;
                """, (snapshot_id,))
                rows = cur.fetchall()
                return [
                    {
                        "cluster_id": r[0],
                        "subcluster_seq": r[1],
                        "pdv_id": r[2],
                        "sequencia_ordem": r[3],
                    }
                    for r in rows
                ]

    # =========================================================
    # 8️⃣ Rotas operacionais
    # =========================================================
    def get_operational_routes(self, tenant_id: int, uf: str = None, cidade: str = None):
        """Retorna as rotas operacionais, com filtros opcionais por UF e cidade."""
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                sql = """
                    SELECT 
                        s.id,
                        s.cluster_id,
                        s.subcluster_seq,
                        s.n_pdvs,
                        s.dist_total_km,
                        s.tempo_total_min,
                        AVG(p.lat) AS centro_lat,
                        AVG(p.lon) AS centro_lon
                    FROM sales_subcluster s
                    JOIN sales_subcluster_pdv p
                      ON p.cluster_id = s.cluster_id
                     AND p.subcluster_seq = s.subcluster_seq
                     AND p.tenant_id = s.tenant_id
                    LEFT JOIN pdvs pd
                      ON pd.id = p.pdv_id
                    WHERE s.tenant_id = %s
                """
                params = [tenant_id]
                if uf:
                    sql += " AND pd.uf = %s"
                    params.append(uf)
                if cidade:
                    sql += " AND LOWER(pd.cidade) = LOWER(%s)"
                    params.append(cidade)

                sql += """
                    GROUP BY 
                        s.id, s.cluster_id, s.subcluster_seq, s.n_pdvs, 
                        s.dist_total_km, s.tempo_total_min
                    ORDER BY s.cluster_id, s.subcluster_seq;
                """

                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
                logger.info(
                    f"📦 {len(rows)} rotas carregadas (tenant={tenant_id}"
                    + (f" | UF={uf}" if uf else "")
                    + (f" | Cidade={cidade}" if cidade else "")
                    + ")"
                )
                return [dict(zip(colnames, row)) for row in rows]

    # =========================================================
    # 9️⃣ Lista de cidades por UF
    # =========================================================
    def get_cidades_por_uf(self, tenant_id: int, uf: str) -> list[str]:
        """Retorna lista única de cidades que possuem PDVs clusterizados na UF informada."""
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT DISTINCT p.cidade
                    FROM pdvs p
                    WHERE p.tenant_id = %s AND UPPER(p.uf) = UPPER(%s)
                      AND p.pdv_lat IS NOT NULL AND p.pdv_lon IS NOT NULL
                    ORDER BY p.cidade;
                """, (tenant_id, uf))
                rows = cur.fetchall()
                cidades = [r[0] for r in rows] if rows else []
                logger.info(f"🌎 {len(cidades)} cidades encontradas (tenant={tenant_id}, UF={uf})")
                return cidades

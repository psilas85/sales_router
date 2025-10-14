# sales_router/src/sales_routing/reporting/route_summary_service.py

import pandas as pd
from dataclasses import dataclass
from typing import Optional, Literal
from loguru import logger


@dataclass
class RouteSummary:
    tenant_id: int
    cluster_id: int
    subcluster_seq: int
    cidade: str
    uf: str
    n_pdvs: int
    dist_total_km: float
    tempo_total_min: float
    avg_tempo_por_pdv: float
    avg_dist_por_pdv: float
    fonte: Literal["operacional", "snapshot"] = "operacional"
    snapshot_nome: Optional[str] = None


class RouteSummaryService:
    """
    Serviço de geração de resumo consolidado das rotas (SalesRouter).
    Compatível com:
      - Rotas operacionais atuais (sales_subcluster / sales_subcluster_pdv / pdvs)
      - Snapshots salvos (sales_routing_snapshot*)
    """

    def __init__(self, conn):
        self.conn = conn

    # =========================================================
    # Gera resumo consolidado de rotas
    # =========================================================
    def gerar_resumo_rotas(
        self,
        tenant_id: int,
        uf: Optional[str] = None,
        cidade: Optional[str] = None,
        snapshot: Optional[str] = None,
    ) -> pd.DataFrame:
        cur = self.conn.cursor()

        # =========================================================
        # 🔹 Snapshot salvo
        # =========================================================
        if snapshot:
            sql = """
                SELECT 
                    t.nome AS snapshot_nome, 
                    s.cluster_id, s.subcluster_seq,
                    s.dist_total_km, s.tempo_total_min,
                    COUNT(p.pdv_id) AS n_pdvs,
                    COALESCE(NULLIF(TRIM(t.uf), ''), 'N/A') AS uf,
                    COALESCE(NULLIF(TRIM(t.cidade), ''), 'N/A') AS cidade
                FROM sales_routing_snapshot_subcluster s
                JOIN sales_routing_snapshot_pdv p
                    ON s.snapshot_id = p.snapshot_id
                AND s.cluster_id = p.cluster_id
                AND s.subcluster_seq = p.subcluster_seq
                JOIN sales_routing_snapshot t 
                    ON s.snapshot_id = t.id
                WHERE t.tenant_id = %s
            """
        # =========================================================
        # 🔹 Última simulação operacional
        # =========================================================
        else:
            sql = """
                SELECT 
                    s.cluster_id, s.subcluster_seq,
                    s.dist_total_km, s.tempo_total_min,
                    COUNT(p.pdv_id) AS n_pdvs,
                    COALESCE(MAX(TRIM(d.cidade)), 'N/A') AS cidade,
                    COALESCE(MAX(TRIM(d.uf)), 'N/A') AS uf
                FROM sales_subcluster s
                JOIN sales_subcluster_pdv p
                    ON s.cluster_id = p.cluster_id
                AND s.subcluster_seq = p.subcluster_seq
                JOIN pdvs d 
                    ON p.pdv_id = d.id
                WHERE s.tenant_id = %s
            """

        params = [tenant_id]
        where_clauses = []

        # =========================================================
        # 🔹 Filtros opcionais (com coringas e insensíveis a caixa)
        # =========================================================
        if uf:
            field = "t.uf" if snapshot else "d.uf"
            where_clauses.append(f"TRIM({field}) ILIKE %s")
            params.append(f"%{uf.strip()}%")

        if cidade:
            field = "t.cidade" if snapshot else "d.cidade"
            where_clauses.append(f"TRIM({field}) ILIKE %s")
            params.append(f"%{cidade.strip()}%")

        if where_clauses:
            sql += " AND " + " AND ".join(where_clauses)

        # =========================================================
        # 🔹 Agrupamento final
        # =========================================================
        if snapshot:
            sql += """
                GROUP BY t.nome, s.cluster_id, s.subcluster_seq, 
                        s.dist_total_km, s.tempo_total_min, t.uf, t.cidade;
            """
        else:
            sql += """
                GROUP BY s.cluster_id, s.subcluster_seq, 
                        s.dist_total_km, s.tempo_total_min;
            """

        # =========================================================
        # 🔹 Execução e formatação
        # =========================================================
        logger.debug(f"SQL executado: {sql}")
        logger.debug(f"Parâmetros: {params}")

        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]

        if not rows:
            logger.warning("⚠️ Nenhuma rota encontrada para os filtros fornecidos.")
            return pd.DataFrame(
                columns=[
                    "tenant_id", "cluster_id", "subcluster_seq",
                    "cidade", "uf", "n_pdvs",
                    "dist_total_km", "tempo_total_min",
                    "avg_tempo_por_pdv", "avg_dist_por_pdv",
                    "fonte", "snapshot_nome",
                ]
            )

        df = pd.DataFrame(rows, columns=cols)

        # =========================================================
        # 🔹 Cálculos complementares
        # =========================================================
        df["avg_tempo_por_pdv"] = (df["tempo_total_min"] / df["n_pdvs"]).round(2)
        df["avg_dist_por_pdv"] = (df["dist_total_km"] / df["n_pdvs"]).round(2)
        df["tenant_id"] = tenant_id
        df["fonte"] = "snapshot" if snapshot else "operacional"
        df["snapshot_nome"] = snapshot

        logger.info(f"📊 {len(df)} rotas resumidas geradas ({'snapshot' if snapshot else 'operacional'})")
        return df

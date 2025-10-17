import pandas as pd
from loguru import logger
from src.database.db_connection import get_connection_context


class RotasVendedoresExport:
    """
    Exporta todas as rotas operacionais com informações completas
    de vendedor, distâncias, tempos e PDVs.
    """

    def __init__(self, tenant_id: int):
        self.tenant_id = tenant_id

    def exportar(self, uf: str = None, cidade: str = None):
        params = [self.tenant_id]
        filtro = ""

        if uf:
            filtro += " AND b.uf = %s"
            params.append(uf)
        if cidade:
            filtro += " AND b.cidade = %s"
            params.append(cidade)

        logger.info(f"📦 Exportando rotas completas (tenant={self.tenant_id}, UF={uf}, Cidade={cidade})...")

        query = f"""
            WITH base AS (
                SELECT DISTINCT ON (sp.cluster_id, sp.subcluster_seq, sp.tenant_id)
                    sp.cluster_id,
                    sp.subcluster_seq,
                    sp.tenant_id,
                    pd.cidade,
                    pd.uf,
                    AVG(pd.pdv_lat) AS centro_lat,
                    AVG(pd.pdv_lon) AS centro_lon
                FROM sales_subcluster_pdv sp
                JOIN pdvs pd ON pd.id = sp.pdv_id
                GROUP BY sp.cluster_id, sp.subcluster_seq, sp.tenant_id, pd.cidade, pd.uf
            )
            SELECT
                s.id AS rota_id,
                s.cluster_id,
                s.subcluster_seq,
                s.vendedor_id,
                COALESCE(b.cidade, '-') AS cidade,
                COALESCE(b.uf, '-') AS uf,
                s.n_pdvs,
                s.dist_total_km,
                s.tempo_total_min,
                COALESCE(b.centro_lat, 0) AS centro_lat,
                COALESCE(b.centro_lon, 0) AS centro_lon,
                s.tenant_id
            FROM sales_subcluster s
            LEFT JOIN base b
                ON b.cluster_id = s.cluster_id
                AND b.subcluster_seq = s.subcluster_seq
                AND b.tenant_id = s.tenant_id
            WHERE s.tenant_id = %s
            {filtro}
            ORDER BY s.vendedor_id, b.cidade, s.cluster_id, s.subcluster_seq;
        """

        with get_connection_context() as conn:
            df = pd.read_sql(query, conn, params=params)

        if df.empty:
            logger.warning("❌ Nenhuma rota encontrada para exportação.")
            return None

        path_csv = f"output/reports/{self.tenant_id}/sales_rotas_vendedores.csv"
        df.to_csv(path_csv, index=False, sep=";", decimal=",", encoding="utf-8-sig")

        logger.success(f"💾 CSV gerado com {len(df)} rotas → {path_csv}")
        return path_csv

# ============================================================
# 📦 src/sales_routing/reporting/export_pdv_vendedor.py
# ============================================================

import os
import pandas as pd
from loguru import logger
from src.database.db_connection import get_connection_context


class ExportPDVVendedorService:
    """
    Exporta um CSV consolidando todos os PDVs vinculados aos seus vendedores,
    com base direta na tabela sales_pdv_vendedor.
    """

    def __init__(self, tenant_id: int, assign_id: str):
        self.tenant_id = tenant_id
        self.assign_id = assign_id
        self.output_dir = f"output/reports/{tenant_id}"
        os.makedirs(self.output_dir, exist_ok=True)

    # =========================================================
    # Exportação principal
    # =========================================================
    def exportar_csv(self):
        """Exporta CSV com todos os PDVs e vendedores vinculados (por assign_id)."""
        logger.info(
            f"📤 Exportando PDVs vinculados aos vendedores "
            f"(tenant={self.tenant_id} | assign_id={self.assign_id})..."
        )

        query = f"""
            SELECT 
                pv.tenant_id,
                pv.assign_id,
                pv.routing_id,
                pv.vendedor_id,
                vb.base_lat,
                vb.base_lon,
                vb.total_rotas,
                vb.total_pdvs,
                pv.pdv_id,
                p.cnpj,
                p.cidade,
                p.uf,
                p.pdv_vendas,          -- ✅ novo campo
                p.criado_em AS pdv_criado_em,
                p.pdv_lat,
                p.pdv_lon,
                pv.cluster_id,
                pv.subcluster_seq
            FROM sales_pdv_vendedor pv
            JOIN pdvs p
              ON pv.pdv_id = p.id
            LEFT JOIN sales_vendedor_base vb
              ON vb.tenant_id = pv.tenant_id
             AND vb.assign_id = pv.assign_id
             AND vb.vendedor_id = pv.vendedor_id
            WHERE pv.tenant_id = {self.tenant_id}
              AND pv.assign_id = '{self.assign_id}'
            ORDER BY pv.vendedor_id, p.cidade, p.uf, p.id;
        """

        with get_connection_context() as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            logger.warning(f"⚠️ Nenhum dado encontrado para assign_id={self.assign_id}.")
            return None

        # 🧾 Corrige e formata o CNPJ com máscara
        if "cnpj" in df.columns:
            df["cnpj"] = (
                df["cnpj"]
                .astype(str)
                .str.replace(r"\.0$", "", regex=True)
                .str.replace(r"[^0-9]", "", regex=True)
                .str.zfill(14)
                .apply(lambda x: f"{x[:2]}.{x[2:5]}.{x[5:8]}/{x[8:12]}-{x[12:]}"))
        
        # 🗂️ Caminho e exportação
        output_path = os.path.join(self.output_dir, f"pdvs_por_vendedor_{self.assign_id}.csv")
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        logger.success(f"✅ CSV exportado com sucesso → {output_path}")
        return output_path


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Exporta CSV de PDVs vinculados a vendedores por assign_id (base sales_pdv_vendedor)"
    )
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--assign_id", type=str, required=True, help="Assign ID (UUID da atribuição)")
    args = parser.parse_args()

    ExportPDVVendedorService(tenant_id=args.tenant, assign_id=args.assign_id).exportar_csv()

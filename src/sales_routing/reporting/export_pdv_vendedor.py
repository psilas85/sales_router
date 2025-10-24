# ============================================================
# 📦 src/sales_routing/reporting/export_pdv_vendedor.py
# ============================================================

import os
import pandas as pd
import psycopg2
from loguru import logger
from src.database.db_connection import get_connection_context


class ExportPDVVendedorService:
    """
    Exporta um CSV consolidando todos os PDVs vinculados aos seus vendedores.
    Leitura baseada na view 'vw_pdv_vendedor'.
    """

    def __init__(self, tenant_id: int):
        self.tenant_id = tenant_id
        self.output_dir = f"output/reports/{tenant_id}"
        os.makedirs(self.output_dir, exist_ok=True)

    def exportar_csv(self):
        """Exporta CSV com todos os PDVs e vendedores vinculados."""
        logger.info(f"📤 Exportando PDVs vinculados aos vendedores (tenant={self.tenant_id})...")

        query = f"""
            SELECT 
                tenant_id,
                vendedor_id,
                vendedor_cidade,
                vendedor_bairro,
                pdv_id,
                cnpj,
                cidade,
                bairro,
                uf,
                cep
            FROM vw_pdv_vendedor
            WHERE tenant_id = {self.tenant_id}
            ORDER BY vendedor_id, cidade, bairro;
            """


        with get_connection_context() as conn:
            df = pd.read_sql(query, conn)

        if df.empty:
            logger.warning("⚠️ Nenhum dado encontrado na view vw_pdv_vendedor.")
            return None

        # 🧾 Corrige e formata o CNPJ com máscara (00.000.000/0000-00)
        if "cnpj" in df.columns:
            df["cnpj"] = (
                df["cnpj"]
                .astype(str)                            # evita notação científica
                .str.replace(r"\.0$", "", regex=True)   # remove sufixos .0
                .str.replace(r"[^0-9]", "", regex=True) # mantém apenas números
                .str.zfill(14)                          # garante 14 dígitos
                .apply(lambda x: f"{x[:2]}.{x[2:5]}.{x[5:8]}/{x[8:12]}-{x[12:]}")
            )

        output_path = os.path.join(self.output_dir, "pdvs_por_vendedor.csv")
        df.to_csv(output_path, index=False, encoding="utf-8-sig")

        logger.success(f"✅ CSV exportado com sucesso → {output_path}")
        return output_path




if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Exporta CSV de PDVs vinculados a vendedores")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    args = parser.parse_args()

    ExportPDVVendedorService(tenant_id=args.tenant).exportar_csv()

#sales_router/src/sales_routing/reporting/export_cluster_summary.py

# ============================================================
# 📊 src/sales_routing/reporting/export_cluster_summary.py
# ============================================================

import os
from pathlib import Path
import pandas as pd
from loguru import logger
from src.database.db_connection import get_connection
from openpyxl.styles import Font, Alignment
from openpyxl.utils import get_column_letter

BASE_OUTPUT = Path("/app/output/reports")  # caminho absoluto no container


def exportar_resumo_cluster(tenant_id: int, routing_id: str):
    """
    Exporta o resumo da view vw_sales_routing_resumo_cluster
    em XLSX com formatação executiva.
    """

    logger.info(
        f"📊 Exportando resumo XLSX | tenant={tenant_id} | routing_id={routing_id}"
    )

    sql = """
        SELECT 
            cluster_id              AS "Cluster",
            qtd_pdvs                AS "PDVs",
            tempo_medio_min         AS "Tempo médio (min)",
            tempo_max_min           AS "Tempo máximo (min)",
            tempo_total_min         AS "Tempo total (min)",
            dist_media_km           AS "Distância média (km)",
            dist_max_km             AS "Distância máxima (km)",
            dist_total_km           AS "Distância total (km)",
            valor_total_vendas      AS "Valor total vendas",
            centro_lat              AS "Latitude centro",
            centro_lon              AS "Longitude centro"
        FROM vw_sales_routing_resumo_cluster
        WHERE tenant_id = %s
          AND routing_id = %s
        ORDER BY cluster_id;
    """

    conn = get_connection()
    try:
        df = pd.read_sql_query(sql, conn, params=(tenant_id, routing_id))

        if df.empty:
            logger.warning("⚠️ Nenhum dado encontrado para exportação.")
            return None

        # 📁 pasta por tenant
        pasta = BASE_OUTPUT / str(tenant_id)
        pasta.mkdir(parents=True, exist_ok=True)

        arquivo = pasta / f"routing_resumo_{routing_id}.xlsx"

        # ===============================
        # 🧾 Escrita EXECUTIVA
        # ===============================
        with pd.ExcelWriter(arquivo, engine="openpyxl") as writer:
            df.to_excel(writer, sheet_name="Resumo por Cluster", index=False)
            ws = writer.book["Resumo por Cluster"]

            # 🔒 Freeze header
            ws.freeze_panes = "A2"

            # 🎨 Header
            header_font = Font(bold=True)
            header_align = Alignment(horizontal="center", vertical="center")

            for cell in ws[1]:
                cell.font = header_font
                cell.alignment = header_align

            # 📐 Largura das colunas
            widths = {
                1: 10,   # Cluster
                2: 8,    # PDVs
                3: 20,
                4: 22,
                5: 22,
                6: 22,
                7: 24,
                8: 24,
                9: 22,
                10: 18,
                11: 18,
            }

            for col_idx, width in widths.items():
                ws.column_dimensions[get_column_letter(col_idx)].width = width

        logger.success(f"✅ XLSX gerado: {arquivo}")
        return str(arquivo)

    finally:
        conn.close()



if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Exporta resumo de clusters (Sales Routing).")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--routing_id", type=str, required=True, help="UUID da roteirização")
    args = parser.parse_args()

    caminho = exportar_resumo_cluster(args.tenant, args.routing_id)
    if caminho:
        print(f"\n📂 Arquivo gerado: {caminho}\n")

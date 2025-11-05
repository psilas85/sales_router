#sales_router/src/sales_routing/reporting/export_cluster_summary.py

# ============================================================
# 📊 src/sales_routing/reporting/export_cluster_summary.py
# ============================================================

import os
import csv
from pathlib import Path
from loguru import logger
from src.database.db_connection import get_connection


def exportar_resumo_cluster(tenant_id: int, routing_id: str):
    """
    Exporta o resumo da view vw_sales_routing_resumo_cluster
    para um arquivo CSV organizado por tenant e routing_id.
    Inclui o campo de valor total de vendas (pdv_vendas).
    """
    try:
        logger.info(f"📊 Exportando resumo de clusters | tenant={tenant_id} | routing_id={routing_id}")

        sql = """
            SELECT 
                tenant_id,
                routing_id,
                cluster_id,
                centro_lat,
                centro_lon,
                qtd_pdvs,
                tempo_medio_min,
                tempo_max_min,
                tempo_total_min,
                dist_media_km,
                dist_max_km,
                dist_total_km,
                valor_total_vendas
            FROM vw_sales_routing_resumo_cluster
            WHERE tenant_id = %s AND routing_id = %s
            ORDER BY cluster_id;
        """

        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id, routing_id))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]

        if not rows:
            logger.warning("⚠️ Nenhum dado encontrado para exportação.")
            return None

        pasta_output = Path(f"output/reports/{tenant_id}")
        pasta_output.mkdir(parents=True, exist_ok=True)
        arquivo_csv = pasta_output / f"routing_resumo_{routing_id}.csv"

        with open(arquivo_csv, "w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(colnames)
            writer.writerows(rows)

        logger.success(f"✅ Resumo exportado: {arquivo_csv} ({len(rows)} registros)")
        return str(arquivo_csv)

    except Exception as e:
        logger.error(f"❌ Erro ao exportar resumo: {e}")
        raise

    finally:
        if conn:
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

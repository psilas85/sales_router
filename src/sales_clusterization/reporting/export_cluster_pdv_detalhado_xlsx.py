#sales_router/src/sales_clusterization/reporting/export_cluster_pdv_detalhado_xlsx.py

# ============================================================
# 📦 src/sales_clusterization/reporting/export_cluster_pdv_detalhado_xlsx.py
# ============================================================

import os
import pandas as pd
import argparse
from loguru import logger
from database.db_connection import get_connection


def exportar_cluster_pdv_detalhado(tenant_id: int, clusterization_id: str):
    logger.info(f"📋 Exportando PDVs detalhados | tenant={tenant_id} | clusterization_id={clusterization_id}")

    conn = get_connection()

    # 🔍 Busca o run_id mais recente vinculado à clusterization_id
    query_run = f"""
        SELECT id AS run_id
        FROM cluster_run
        WHERE tenant_id = {tenant_id} AND clusterization_id = '{clusterization_id}'
        ORDER BY criado_em DESC
        LIMIT 1;
    """
    run_df = pd.read_sql_query(query_run, conn)
    if run_df.empty:
        conn.close()
        raise ValueError(f"❌ Nenhum run encontrado para clusterization_id={clusterization_id}")

    run_id = int(run_df.iloc[0]['run_id'])
    logger.info(f"🔎 Run identificado: {run_id}")

    # 📋 Query principal
    query = f"""
        SELECT *
        FROM v_cluster_pdv_detalhado
        WHERE tenant_id = {tenant_id} AND run_id = {run_id}
        ORDER BY cluster_label, pdv_id;
    """

    df = pd.read_sql_query(query, conn)
    conn.close()

    if df.empty:
        raise ValueError(f"❌ Nenhum dado encontrado em v_cluster_pdv_detalhado para run_id={run_id}")

    # =========================================
    # 🔧 Correção: evitar notação científica
    # =========================================
    if "pdv_vendas" in df.columns:
        df["pdv_vendas"] = pd.to_numeric(df["pdv_vendas"], errors="coerce").round(2)


    output_dir = f"output/reports/{tenant_id}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir, f"cluster_pdv_detalhado_{clusterization_id}.xlsx"
    )

    df.to_excel(output_path, index=False)
    logger.success(f"✅ Arquivo salvo em: {output_path}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exporta PDVs detalhados por cluster (SalesRouter)")
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant")
    parser.add_argument("--clusterization_id", type=str, required=True, help="UUID da clusterização")
    args = parser.parse_args()

    exportar_cluster_pdv_detalhado(args.tenant_id, args.clusterization_id)

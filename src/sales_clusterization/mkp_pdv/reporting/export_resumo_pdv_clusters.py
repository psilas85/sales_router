# sales_router/src/sales_clusterization/mkp_pdv/reporting/export_resumo_pdv_clusters.py

# ============================================================
# 📦 sales_clusterization/mkp_pdv/reporting/export_resumo_pdv_clusters.py
# ============================================================

import os
import pandas as pd
import argparse
import csv
from loguru import logger
from database.db_connection import get_connection


def wrap_excel(value):
    """Evita que o Excel formate número como notação científica ou arredonde."""
    if value is None or value == "":
        return ""
    return f'="{value}"'


def exportar_resumo_pdv_clusters(tenant_id: int, clusterization_id: str):

    sql = """
        SELECT 
            p.cluster_id,

            c.centro_bandeira,
            c.centro_cliente,
            c.centro_cnpj,
            c.centro_bairro,

            COUNT(*) AS qtd_pdvs,
            SUM(p.pdv_vendas) AS vendas_totais,

            ROUND(AVG(p.distancia_km)::numeric, 2) AS dist_media_km,
            ROUND(MAX(p.distancia_km)::numeric, 2) AS dist_max_km,

            ROUND(AVG(p.tempo_min)::numeric, 2) AS tempo_medio_min,
            ROUND(MAX(p.tempo_min)::numeric, 2) AS tempo_max_min,

            ROUND(AVG(p.cluster_lat)::numeric, 6) AS cluster_lat,
            ROUND(AVG(p.cluster_lon)::numeric, 6) AS cluster_lon

        FROM mkp_cluster_pdv p
        LEFT JOIN mkp_cluster_centros c
          ON c.clusterization_id = p.clusterization_id
         AND c.cluster_id        = p.cluster_id
         AND c.tenant_id         = p.tenant_id

        WHERE p.tenant_id = %s
          AND p.clusterization_id = %s

        GROUP BY 
            p.cluster_id,
            c.centro_bandeira,
            c.centro_cliente,
            c.centro_cnpj,
            c.centro_bairro
        ORDER BY p.cluster_id;
    """

    conn = get_connection()
    df = pd.read_sql(sql, conn, params=(tenant_id, clusterization_id))
    conn.close()

    if df.empty:
        logger.warning(f"⚠️ Nenhum registro encontrado para clusterization_id={clusterization_id}")
        return None

    # ------------------------------------------------------------
    # 🔧 FORMATAÇÕES PROTEGIDAS PARA EXCEL
    # ------------------------------------------------------------
    df["centro_cnpj"] = df["centro_cnpj"].astype(str).apply(wrap_excel)
    df["cluster_lat"] = df["cluster_lat"].astype(str).apply(wrap_excel)
    df["cluster_lon"] = df["cluster_lon"].astype(str).apply(wrap_excel)

    # ------------------------------------------------------------
    # 💾 SALVAR CSV
    # ------------------------------------------------------------
    output_dir = os.path.join("output", "reports", str(tenant_id))
    os.makedirs(output_dir, exist_ok=True)

    output_path = os.path.join(
        output_dir,
        f"mkp_resumo_pdv_clusters_{clusterization_id}.csv"
    )

    df.to_csv(
        output_path,
        index=False,
        sep=";",
        encoding="utf-8-sig",
        quoting=csv.QUOTE_MINIMAL
    )

    logger.success(f"✅ Resumo de clusters exportado para {output_path}")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exportar resumo PDV MKP para CSV")
    parser.add_argument("--tenant_id", required=True, type=int)
    parser.add_argument("--clusterization_id", required=True)
    args = parser.parse_args()

    exportar_resumo_pdv_clusters(args.tenant_id, args.clusterization_id)

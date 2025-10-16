# src/sales_clusterization/visualization/cluster_noise_heatmap.py

import folium
from folium.plugins import HeatMap
from loguru import logger
from src.database.db_connection import get_connection
import argparse
import os


def gerar_heatmap_ruido(tenant_id: int, run_id: int):
    """
    Gera mapa de calor (heatmap) dos PDVs de ruído (label=-1) no último run DBSCAN.
    """
    logger.info(f"🔥 Gerando heatmap de ruídos | tenant_id={tenant_id} | run_id={run_id}")

    sql = """
        SELECT lat, lon
        FROM cluster_setor_pdv
        WHERE tenant_id = %s
          AND run_id = %s
          AND cluster_id IS NULL;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (tenant_id, run_id))
            rows = cur.fetchall()

    if not rows:
        logger.warning("⚠️ Nenhum PDV de ruído encontrado (todos foram clusterizados).")
        return

    pontos = [(float(r[0]), float(r[1])) for r in rows if r[0] and r[1]]

    m = folium.Map(location=[-22.9, -43.2], zoom_start=7, tiles="CartoDB positron")

    HeatMap(
        pontos,
        radius=10,
        blur=15,
        min_opacity=0.4,
        max_zoom=10,
    ).add_to(m)

    output_dir = f"output/maps/{tenant_id}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = f"{output_dir}/cluster_noise_heatmap_run{run_id}.html"

    m.save(output_path)
    logger.success(f"✅ Heatmap salvo em {output_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Gera heatmap dos PDVs de ruído (não clusterizados)")
    parser.add_argument("--tenant_id", type=int, required=True)
    parser.add_argument("--run_id", type=int, required=True)
    args = parser.parse_args()

    gerar_heatmap_ruido(args.tenant_id, args.run_id)

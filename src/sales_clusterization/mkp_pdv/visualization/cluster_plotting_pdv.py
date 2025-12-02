# ============================================================
# 📦 sales_clusterization/mkp_pdv/visualization/cluster_plotting_pdv.py
# ============================================================

import folium
import random
import argparse
import webbrowser
import pandas as pd
from pathlib import Path
from loguru import logger
from database.db_connection import get_connection


def buscar_dados_pdv_clusterizados(tenant_id, clusterization_id):
    sql = """
        SELECT 
            cluster_id,
            pdv_id,
            cnpj,
            lat,
            lon,
            pdv_vendas,
            cluster_lat,
            cluster_lon,
            cluster_bairro,
            distancia_km,
            tempo_min
        FROM mkp_cluster_pdv
        WHERE tenant_id=%s AND clusterization_id=%s;
    """

    conn = get_connection()
    df = pd.read_sql(sql, conn, params=(tenant_id, clusterization_id))
    conn.close()
    return df


def gerar_mapa_pdv_clusters(df, output_path: Path):

    if df.empty:
        logger.warning("❌ Nenhum PDV encontrado para este clusterization_id.")
        return

    lat_c = df["cluster_lat"].mean()
    lon_c = df["cluster_lon"].mean()

    m = folium.Map(location=[lat_c, lon_c], zoom_start=11, tiles="CartoDB positron")

    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728",
        "#9467bd", "#8c564b", "#e377c2", "#7f7f7f",
        "#bcbd22", "#17becf"
    ]

    grupos = df.groupby("cluster_id")

    for idx, (cluster_id, grupo) in enumerate(sorted(grupos)):
        cor = palette[idx % len(palette)]

        centro_lat = float(grupo["cluster_lat"].iloc[0])
        centro_lon = float(grupo["cluster_lon"].iloc[0])
        bairro = grupo["cluster_bairro"].iloc[0] or "N/D"

        # 🔵 PDVs
        for _, r in grupo.iterrows():
            jitter_lat = float(r["lat"]) + random.uniform(-0.002, 0.002)
            jitter_lon = float(r["lon"]) + random.uniform(-0.002, 0.002)

            vendas = r["pdv_vendas"] or 0
            radius = max(4, min(14, 4 + vendas * 0.2))

            popup = f"""
            <b>Cluster:</b> {cluster_id}<br>
            <b>Bairro:</b> {bairro}<br>
            <b>CNPJ:</b> {r['cnpj']}<br>
            <b>PDV ID:</b> {r['pdv_id']}<br>
            <b>Vendas:</b> {vendas}<br>
            <b>Distância:</b> {r['distancia_km']:.2f} km<br>
            <b>Tempo:</b> {r['tempo_min']:.1f} min<br>
            """

            folium.CircleMarker(
                location=(jitter_lat, jitter_lon),
                radius=radius,
                color=cor,
                fill=True,
                fill_opacity=0.75,
                popup=popup,
                tooltip=f"PDV {r['pdv_id']} | vendas={vendas}"
            ).add_to(m)

        # ⚫ Centro do cluster
        folium.CircleMarker(
            location=(centro_lat, centro_lon),
            radius=10,
            color="black",
            weight=2,
            fill=True,
            fill_color=cor,
            fill_opacity=1,
            popup=f"<b>Cluster {cluster_id}</b><br>{bairro}",
            tooltip=f"Cluster {cluster_id}"
        ).add_to(m)

    if output_path.exists():
        output_path.unlink()

    m.save(output_path)
    logger.success(f"✅ Mapa salvo em {output_path}")


def main():
    parser = argparse.ArgumentParser(description="Gerar mapa PDV MKP por clusterization_id")
    parser.add_argument("--tenant_id", type=int, required=True)
    parser.add_argument("--clusterization_id", required=True)
    parser.add_argument("--abrir", action="store_true")
    args = parser.parse_args()

    df = buscar_dados_pdv_clusterizados(args.tenant_id, args.clusterization_id)

    output_dir = Path(f"output/maps/{args.tenant_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    html_path = output_dir / f"mkp_pdv_cluster_{args.clusterization_id}.html"

    gerar_mapa_pdv_clusters(df, html_path)

    if args.abrir:
        try:
            webbrowser.open_new_tab(html_path.resolve().as_uri())
        except Exception:
            pass


if __name__ == "__main__":
    main()

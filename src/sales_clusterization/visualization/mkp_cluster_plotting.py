# =========================================================
# 📦 src/sales_clusterization/visualization/cluster_plotting_mkp.py
# =========================================================

import folium
import argparse
import webbrowser
from pathlib import Path
from loguru import logger
from src.database.db_connection import get_connection


# =========================================================
# 1️⃣ BUSCA DE DADOS
# =========================================================

def buscar_dados_clusterizados(tenant_id: int, clusterization_id: str):
    """
    Retorna os CEPs clusterizados do marketplace (mkp_cluster_cep)
    com coordenadas, distâncias e quantidades de clientes.
    """
    sql = """
        SELECT 
            cluster_id,
            cep,
            clientes_total,
            clientes_target,
            cluster_lat,
            cluster_lon,
            distancia_km,
            tempo_min,
            is_outlier
        FROM mkp_cluster_cep
        WHERE tenant_id = %s
          AND clusterization_id = %s;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (tenant_id, clusterization_id))
        rows = cur.fetchall()
    conn.close()
    return rows


# =========================================================
# 2️⃣ FUNÇÃO DE PLOTAGEM
# =========================================================

def gerar_mapa_mkp_clusters(dados, output_path: Path):
    """
    Gera mapa HTML com clusters de CEPs (mkp_cluster_cep).
    - Marcadores com leve jitter visual (dispersion) para diferenciar CEPs.
    - Círculos coloridos por cluster_id.
    - Centro do cluster destacado com círculo maior preto.
    """
    import math
    import random
    import pandas as pd

    if not dados:
        logger.warning("❌ Nenhum dado encontrado em mkp_cluster_cep.")
        return

    df = pd.DataFrame(
        dados,
        columns=[
            "cluster_id", "cep", "clientes_total", "clientes_target",
            "cluster_lat", "cluster_lon", "distancia_km", "tempo_min", "is_outlier"
        ]
    )

    # Coordenadas centrais para inicializar o mapa
    lat_centro = df["cluster_lat"].astype(float).mean()
    lon_centro = df["cluster_lon"].astype(float).mean()
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=8, tiles="CartoDB positron")

    clusters = df.groupby("cluster_id")
    random.seed(42)
    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
    ]

    for i, (cluster_id, grupo) in enumerate(sorted(clusters)):
        cor = palette[i % len(palette)]
        centro_lat = float(grupo["cluster_lat"].iloc[0])
        centro_lon = float(grupo["cluster_lon"].iloc[0])
        total_clientes = int(grupo["clientes_total"].sum())
        total_target = int(grupo["clientes_target"].sum())

        # 🔹 Plotagem dos CEPs com jitter
        for _, row in grupo.iterrows():
            base_lat = float(row["cluster_lat"])
            base_lon = float(row["cluster_lon"])

            # Jitter leve para dispersão visual (±0.015 ≈ até ~1.5 km)
            lat = base_lat + random.uniform(-0.015, 0.015)
            lon = base_lon + random.uniform(-0.015, 0.015)

            clientes_total = int(row["clientes_total"])
            clientes_target = int(row["clientes_target"])
            base_val = clientes_target if clientes_target > 0 else clientes_total
            radius = max(3, min(12, 3 + base_val * 0.6))

            tooltip_text = f"CEP {row['cep']} | Clientes={clientes_total} | Target={clientes_target}"
            popup_html = f"""
            <b>Cluster:</b> {cluster_id}<br>
            <b>CEP:</b> {row['cep']}<br>
            <b>Clientes Total:</b> {clientes_total}<br>
            <b>Clientes Target:</b> {clientes_target}<br>
            <b>Distância:</b> {row['distancia_km']:.2f} km<br>
            <b>Tempo:</b> {row['tempo_min']:.1f} min
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=radius,
                color=cor,
                fill=True,
                fill_opacity=0.8,
                popup=folium.Popup(popup_html, max_width=320),
                tooltip=folium.Tooltip(tooltip_text, sticky=True)
            ).add_to(m)

        # 🔸 Ponto central do cluster
        folium.CircleMarker(
            location=(centro_lat, centro_lon),
            radius=10,
            color="black",
            weight=2,
            fill=True,
            fill_color=cor,
            fill_opacity=0.9,
            popup=folium.Popup(
                f"<b>Centro do Cluster {cluster_id}</b><br>"
                f"Total Clientes: {total_clientes}<br>"
                f"Target: {total_target}",
                max_width=250
            ),
        ).add_to(m)

    # 🔹 Legenda
    legend_html = """
    <div style="
        position: fixed; bottom: 50px; left: 50px; width: 220px;
        z-index:9999; font-size:14px; background-color:white;
        border:2px solid grey; border-radius:8px; padding:10px;">
        <b>Clusters (mkp)</b><br>{}
    </div>
    """.format("<br>".join([
        f"<span style='color:{palette[i % len(palette)]}'>●</span> Cluster {cluster_id}"
        for i, cluster_id in enumerate(sorted(df['cluster_id'].unique()))
    ]))
    m.get_root().html.add_child(folium.Element(legend_html))

    if output_path.exists():
        output_path.unlink()
    m.save(output_path)
    logger.success(f"✅ Mapa de clusterização marketplace salvo em {output_path}")


# =========================================================
# 3️⃣ MAIN CLI
# =========================================================

def main():
    parser = argparse.ArgumentParser(
        description="Gerar mapa de clusterização de CEPs do marketplace (mkp_cluster_cep) via clusterization_id"
    )
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant")
    parser.add_argument("--clusterization_id", type=str, required=True, help="UUID da clusterização")
    parser.add_argument("--modo_interativo", action="store_true", help="Abre o mapa no navegador (fora do Docker)")
    args = parser.parse_args()

    logger.info(f"🗺️ Gerando mapa MKP | tenant_id={args.tenant_id} | clusterization_id={args.clusterization_id}")

    output_dir = Path(f"output/maps/{args.tenant_id}")
    output_dir.mkdir(parents=True, exist_ok=True)
    mapa_html = output_dir / f"mkp_cluster_{args.clusterization_id}.html"

    dados = buscar_dados_clusterizados(args.tenant_id, args.clusterization_id)
    gerar_mapa_mkp_clusters(dados, mapa_html)

    if args.modo_interativo:
        try:
            webbrowser.open_new_tab(mapa_html.resolve().as_uri())
        except Exception as e:
            logger.warning(f"⚠️ Não foi possível abrir o navegador automaticamente: {e}")


if __name__ == "__main__":
    main()

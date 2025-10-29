# ============================================================
# 📦 src/sales_clusterization/visualization/cluster_plotting_mkp.py
# ============================================================

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
    com coordenadas, distâncias e quantidades de clientes,
    incluindo o bairro central do cluster.
    """
    sql = """
        SELECT 
            cluster_id,
            cep,
            clientes_total,
            clientes_target,
            cluster_lat,
            cluster_lon,
            cluster_bairro,
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
    - Marcadores coloridos por cluster_id.
    - Bairro central exibido no popup do cluster.
    - Centro do cluster destacado com círculo preto.
    """
    import random
    import pandas as pd

    if not dados:
        logger.warning("❌ Nenhum dado encontrado em mkp_cluster_cep.")
        return

    df = pd.DataFrame(
        dados,
        columns=[
            "cluster_id", "cep", "clientes_total", "clientes_target",
            "cluster_lat", "cluster_lon", "cluster_bairro",
            "distancia_km", "tempo_min", "is_outlier"
        ]
    )

    # Coordenadas médias para centralizar o mapa
    lat_centro = df["cluster_lat"].astype(float).mean()
    lon_centro = df["cluster_lon"].astype(float).mean()
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=9, tiles="CartoDB positron")

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
        cluster_bairro = grupo["cluster_bairro"].iloc[0] or "N/D"
        total_clientes = int(grupo["clientes_total"].sum())
        total_target = int(grupo["clientes_target"].sum())

        # 🔹 Plotagem dos CEPs (com leve jitter para melhor visualização)
        for _, row in grupo.iterrows():
            lat = float(row["cluster_lat"]) + random.uniform(-0.010, 0.010)
            lon = float(row["cluster_lon"]) + random.uniform(-0.010, 0.010)

            clientes_total = int(row["clientes_total"])
            clientes_target = int(row["clientes_target"])
            base_val = clientes_target if clientes_target > 0 else clientes_total
            radius = max(3, min(12, 3 + base_val * 0.6))

            tooltip_text = f"CEP {row['cep']} | Clientes={clientes_total} | Target={clientes_target}"
            popup_html = f"""
            <b>Cluster:</b> {cluster_id}<br>
            <b>Bairro:</b> {cluster_bairro}<br>
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
                fill_opacity=0.75,
                popup=folium.Popup(popup_html, max_width=320),
                tooltip=folium.Tooltip(tooltip_text, sticky=True)
            ).add_to(m)

        # 🔸 Centro do cluster (ponto preto com popup do bairro)
        popup_cluster = f"""
        <b>Cluster {cluster_id}</b><br>
        <b>Bairro Central:</b> {cluster_bairro}<br>
        <b>Total Clientes:</b> {total_clientes}<br>
        <b>Target:</b> {total_target}
        """

        folium.CircleMarker(
            location=(centro_lat, centro_lon),
            radius=10,
            color="black",
            weight=2,
            fill=True,
            fill_color=cor,
            fill_opacity=0.9,
            popup=folium.Popup(popup_cluster, max_width=280),
            tooltip=folium.Tooltip(f"Cluster {cluster_id} - {cluster_bairro}", sticky=True)
        ).add_to(m)

    # ============================================================
    # 🔹 Legenda (até 10 clusters)
    # ============================================================
    num_clusters = df["cluster_id"].nunique()
    if num_clusters <= 30:
        legend_html = """
        <div style="
            position: fixed; bottom: 50px; left: 50px; width: 250px;
            z-index:9999; font-size:14px; background-color:white;
            border:2px solid grey; border-radius:8px; padding:10px;">
            <b>Clusters (MKP)</b><br>{}
        </div>
        """.format("<br>".join([
            f"<span style='color:{palette[i % len(palette)]}'>●</span> "
            f"Cluster {cluster_id} – {df[df['cluster_id'] == cluster_id]['cluster_bairro'].iloc[0] or 'N/D'}"
            for i, cluster_id in enumerate(sorted(df['cluster_id'].unique()))
        ]))
        m.get_root().html.add_child(folium.Element(legend_html))
    else:
        logger.info(f"📊 Legenda omitida ({num_clusters} clusters detectados).")

    # ============================================================
    # 💾 Salvamento
    # ============================================================
    if output_path.exists():
        output_path.unlink()
    m.save(output_path)
    logger.success(f"✅ Mapa de clusterização marketplace salvo em {output_path}")


# =========================================================
# 3️⃣ MAIN CLI
# =========================================================

def main():
    parser = argparse.ArgumentParser(
        description="Gerar mapa de clusterização de CEPs (mkp_cluster_cep) com bairro central via clusterization_id"
    )
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant")
    parser.add_argument("--clusterization_id", type=str, required=True, help="UUID da clusterização")
    parser.add_argument("--modo_interativo", action="store_true", help="Abre o mapa no navegador")
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

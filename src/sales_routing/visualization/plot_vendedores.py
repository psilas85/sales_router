# src/sales_routing/visualization/plot_vendedores.py
# src/sales_routing/visualization/plot_vendedores.py

import os
import folium
import argparse
import pandas as pd
from loguru import logger
from src.database.db_connection import get_connection_context


# =========================================================
# 1️⃣ Leitura das rotas com vendedor_id
# =========================================================
def buscar_rotas_com_vendedor(tenant_id: int, uf: str = None):
    """Carrega rotas operacionais com vendedor_id e coordenadas médias."""
    with get_connection_context() as conn:
        with conn.cursor() as cur:
            sql = """
                SELECT 
                    s.id,
                    s.cluster_id,
                    s.subcluster_seq,
                    s.vendedor_id,
                    AVG(p.lat) AS centro_lat,
                    AVG(p.lon) AS centro_lon,
                    s.n_pdvs,
                    s.dist_total_km,
                    s.tempo_total_min
                FROM sales_subcluster s
                JOIN sales_subcluster_pdv p
                  ON p.cluster_id = s.cluster_id
                 AND p.subcluster_seq = s.subcluster_seq
                 AND p.tenant_id = s.tenant_id
                LEFT JOIN pdvs pd
                  ON pd.id = p.pdv_id
                WHERE s.tenant_id = %s
                  AND s.vendedor_id IS NOT NULL
            """
            params = [tenant_id]
            if uf:
                sql += " AND pd.uf = %s"
                params.append(uf)

            sql += """
                GROUP BY s.id, s.cluster_id, s.subcluster_seq, s.vendedor_id,
                         s.n_pdvs, s.dist_total_km, s.tempo_total_min
                ORDER BY s.vendedor_id, s.cluster_id, s.subcluster_seq;
            """

            cur.execute(sql, tuple(params))
            rows = cur.fetchall()
            colnames = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=colnames)

            logger.info(f"📦 {len(df)} rotas com vendedor_id carregadas (tenant={tenant_id}, UF={uf or 'todas'})")
            return df


# =========================================================
# 2️⃣ Leitura das bases calculadas
# =========================================================
def buscar_bases_vendedores(tenant_id: int):
    """Carrega as bases (bairro/cidade) calculadas de cada vendedor."""
    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT vendedor_id, base_bairro, base_cidade, base_lat, base_lon
                FROM sales_vendedor_base
                WHERE tenant_id = %s;
            """, (tenant_id,))
            rows = cur.fetchall()
            if not rows:
                logger.warning("⚠️ Nenhuma base de vendedor encontrada.")
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=["vendedor_id", "base_bairro", "base_cidade", "base_lat", "base_lon"])
            logger.info(f"🏠 {len(df)} bases de vendedores carregadas (tenant={tenant_id})")
            return df


# =========================================================
# 3️⃣ Geração do mapa interativo
# =========================================================
def gerar_mapa_vendedores(rotas_df: pd.DataFrame, bases_df: pd.DataFrame, tenant_id: int, uf: str = None):
    """Gera mapa Folium colorido por vendedor_id, com bases destacadas."""
    if rotas_df.empty:
        logger.warning("⚠️ Nenhuma rota com vendedor_id encontrada.")
        return None

    # Define centro do mapa
    lat_centro = bases_df["base_lat"].mean() if not bases_df.empty else rotas_df["centro_lat"].mean()
    lon_centro = bases_df["base_lon"].mean() if not bases_df.empty else rotas_df["centro_lon"].mean()
    mapa = folium.Map(location=[lat_centro, lon_centro], zoom_start=7, tiles="CartoDB positron")

    # Paleta de cores (rotativa)
    cores = [
        "#FF5733", "#33FF57", "#3357FF", "#F7DC6F", "#BB8FCE", "#17A589",
        "#E67E22", "#5DADE2", "#E74C3C", "#2ECC71", "#AF7AC5", "#1ABC9C",
        "#F1C40F", "#7DCEA0", "#2980B9", "#BA4A00", "#6C3483", "#1F618D",
        "#F39C12", "#27AE60", "#C0392B", "#8E44AD", "#34495E", "#D35400",
    ]
    vendedores = sorted(rotas_df["vendedor_id"].unique())
    color_map = {vid: cores[i % len(cores)] for i, vid in enumerate(vendedores)}

    # 🔹 Marcadores das rotas (centroides)
    for _, row in rotas_df.iterrows():
        cor = color_map.get(row["vendedor_id"], "#000000")
        folium.CircleMarker(
            location=[row["centro_lat"], row["centro_lon"]],
            radius=3,
            color=cor,
            fill=True,
            fill_opacity=0.6,
            popup=(
                f"<b>Vendedor:</b> {row['vendedor_id']}<br>"
                f"<b>Cluster:</b> {row['cluster_id']} / Sub: {row['subcluster_seq']}<br>"
                f"<b>PDVs:</b> {row['n_pdvs']}<br>"
                f"<b>Distância:</b> {row['dist_total_km']:.1f} km<br>"
                f"<b>Tempo:</b> {row['tempo_total_min']:.1f} min"
            )
        ).add_to(mapa)

    # 🔴 Marcadores das bases dos vendedores
    if not bases_df.empty:
        for _, row in bases_df.iterrows():
            folium.CircleMarker(
                location=[row["base_lat"], row["base_lon"]],
                radius=7,
                color="blue",
                fill=True,
                fill_opacity=0.8,
                popup=(
                    f"<b>🏠 Vendedor {int(row['vendedor_id'])}</b><br>"
                    f"<b>Bairro:</b> {row['base_bairro'] or 'N/D'}<br>"
                    f"<b>Cidade:</b> {row['base_cidade'] or 'N/D'}"
                )
            ).add_to(mapa)

    # 🔹 Legenda HTML
    legenda_html = """
    <div style="position: fixed; bottom: 40px; left: 40px; width: 220px; height: auto;
                background-color: white; border:2px solid grey; z-index:9999;
                font-size:12px; overflow-y:auto; max-height:300px; padding:10px;">
        <b>🧍‍♂️ Vendedores</b><br>
    """
    for vid, cor in color_map.items():
        legenda_html += f"<div><span style='color:{cor};'>■</span> Vendedor {vid}</div>"
    legenda_html += "<hr><div><span style='color:blue;'>⬤</span> Bases (Bairros)</div></div>"
    mapa.get_root().html.add_child(folium.Element(legenda_html))

    # Caminho de saída fixo (sobrescreve sempre)
    # Caminho de saída por tenant (ex: sales_router/output/maps/1/sales_vendedores.html)
    # Caminho relativo ao diretório de trabalho do container
    pasta_output = os.path.join("output", "maps", str(tenant_id))

    os.makedirs(pasta_output, exist_ok=True)

    caminho = os.path.join(pasta_output, "sales_vendedores.html")
    mapa.save(caminho)

    logger.success(f"🗺️ Mapa salvo em: {caminho} (sobrescrita automática por tenant)")

    return caminho


# =========================================================
# 4️⃣ Execução via CLI
# =========================================================
def main():
    parser = argparse.ArgumentParser(description="Gera mapa interativo das bases de vendedores (bairros)")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--uf", type=str, help="UF opcional (ex: RJ)")
    args = parser.parse_args()

    logger.info(f"🗺️ Gerando mapa de vendedores | Tenant={args.tenant} | UF={args.uf or 'todas'}")

    rotas_df = buscar_rotas_com_vendedor(args.tenant, args.uf)
    bases_df = buscar_bases_vendedores(args.tenant)

    if rotas_df.empty:
        logger.warning("❌ Nenhuma rota encontrada para o filtro informado.")
        return

    gerar_mapa_vendedores(rotas_df, bases_df, args.tenant, args.uf)


if __name__ == "__main__":
    main()

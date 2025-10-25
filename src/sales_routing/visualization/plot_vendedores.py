# src/sales_routing/visualization/plot_vendedores.py

# =========================================================
# 📦 src/sales_routing/visualization/plot_vendedores.py
# =========================================================

import os
import folium
import argparse
import pandas as pd
from loguru import logger
from src.database.db_connection import get_connection_context


# =========================================================
# 1️⃣ Buscar PDVs atribuídos a vendedores (por assign_id)
# =========================================================
def buscar_pdvs_por_assign(tenant_id: int, assign_id: str):
    """Carrega PDVs vinculados aos vendedores para o assign_id informado."""
    with get_connection_context() as conn:
        with conn.cursor() as cur:
            sql = """
                SELECT 
                    v.vendedor_id,
                    pd.pdv_lat,
                    pd.pdv_lon,
                    pd.bairro,
                    pd.cidade,
                    pd.uf
                FROM sales_subcluster_vendedor v
                JOIN sales_subcluster_pdv sp
                  ON v.tenant_id = sp.tenant_id
                 AND v.cluster_id = sp.cluster_id
                 AND v.subcluster_seq = sp.subcluster_seq
                JOIN pdvs pd
                  ON pd.id = sp.pdv_id
                WHERE v.tenant_id = %s
                  AND v.assign_id = %s
                  AND pd.pdv_lat IS NOT NULL
                  AND pd.pdv_lon IS NOT NULL;
            """
            cur.execute(sql, (tenant_id, assign_id))
            rows = cur.fetchall()
            if not rows:
                logger.warning(f"⚠️ Nenhum PDV encontrado para tenant={tenant_id}, assign_id={assign_id}")
                return pd.DataFrame()
            colnames = [desc[0] for desc in cur.description]
            df = pd.DataFrame(rows, columns=colnames)
            logger.info(f"📦 {len(df)} PDVs carregados (tenant={tenant_id}, assign_id={assign_id})")
            return df


# =========================================================
# 2️⃣ Buscar bases de vendedores (filtradas por assign_id)
# =========================================================
def buscar_bases_vendedores(tenant_id: int, assign_id: str):
    """Carrega bases geométricas dos vendedores vinculadas ao assign_id informado."""
    with get_connection_context() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT vendedor_id, base_lat, base_lon, total_rotas, total_pdvs
                FROM sales_vendedor_base
                WHERE tenant_id = %s
                  AND assign_id = %s;
            """, (tenant_id, assign_id))
            rows = cur.fetchall()
            if not rows:
                logger.warning(f"⚠️ Nenhuma base de vendedor encontrada (tenant={tenant_id}, assign_id={assign_id})")
                return pd.DataFrame()
            df = pd.DataFrame(rows, columns=["vendedor_id", "base_lat", "base_lon", "total_rotas", "total_pdvs"])
            logger.info(f"🏠 {len(df)} bases carregadas (tenant={tenant_id}, assign_id={assign_id})")
            return df


# =========================================================
# 3️⃣ Geração do mapa com PDVs e bases
# =========================================================
def gerar_mapa_vendedores(pdvs_df: pd.DataFrame, bases_df: pd.DataFrame, tenant_id: int, assign_id: str):
    """Gera mapa Folium colorido por vendedor, mostrando PDVs e bases geométricas."""
    if pdvs_df.empty:
        logger.warning("❌ Nenhum PDV encontrado para plotagem.")
        return None

    # Centro do mapa (média geral)
    lat_centro = pdvs_df["pdv_lat"].mean()
    lon_centro = pdvs_df["pdv_lon"].mean()
    mapa = folium.Map(location=[lat_centro, lon_centro], zoom_start=8, tiles="CartoDB positron")

    # Paleta de cores
    cores = [
        "#FF5733", "#33FF57", "#3357FF", "#F7DC6F", "#BB8FCE", "#17A589",
        "#E67E22", "#5DADE2", "#E74C3C", "#2ECC71", "#AF7AC5", "#1ABC9C",
        "#F1C40F", "#7DCEA0", "#2980B9", "#BA4A00", "#6C3483", "#1F618D",
        "#F39C12", "#27AE60", "#C0392B", "#8E44AD", "#34495E", "#D35400"
    ]
    vendedores = sorted(pdvs_df["vendedor_id"].unique())
    color_map = {vid: cores[i % len(cores)] for i, vid in enumerate(vendedores)}

    # 🔹 PDVs coloridos por vendedor
    for _, row in pdvs_df.iterrows():
        cor = color_map.get(row["vendedor_id"], "#000000")
        folium.CircleMarker(
            location=[row["pdv_lat"], row["pdv_lon"]],
            radius=3,
            color=cor,
            fill=True,
            fill_opacity=0.6,
            popup=(f"<b>Vendedor:</b> {row['vendedor_id']}<br>"
                   f"<b>Cidade:</b> {row['cidade']} - {row['uf']}<br>"
                   f"<b>Bairro:</b> {row['bairro'] or 'N/D'}")
        ).add_to(mapa)

    # 🔴 Bases geométricas dos vendedores
    if not bases_df.empty:
        for _, row in bases_df.iterrows():
            cor = color_map.get(row["vendedor_id"], "#000000")
            folium.RegularPolygonMarker(
                location=[row["base_lat"], row["base_lon"]],
                number_of_sides=4,
                radius=8,
                color="black",
                weight=2,
                fill_color=cor,
                fill_opacity=1,
                popup=(f"<b>🏠 Base Vendedor {int(row['vendedor_id'])}</b><br>"
                       f"Rotas: {row['total_rotas']}<br>"
                       f"PDVs: {row['total_pdvs']}")
            ).add_to(mapa)

    # 🔸 Legenda
    legenda_html = """
    <div style="position: fixed; bottom: 40px; left: 40px; width: 240px;
                background-color: white; border:2px solid grey; z-index:9999;
                font-size:12px; padding:10px;">
        <b>🧍‍♂️ Vendedores</b><br>
    """
    for vid, cor in color_map.items():
        legenda_html += f"<div><span style='color:{cor};'>■</span> Vendedor {vid}</div>"
    legenda_html += "<hr><div><span style='color:black;'>⬛</span> Bases dos vendedores</div></div>"
    mapa.get_root().html.add_child(folium.Element(legenda_html))

    # Caminho de saída
    pasta_output = os.path.join("output", "maps", str(tenant_id))
    os.makedirs(pasta_output, exist_ok=True)
    caminho = os.path.join(pasta_output, f"vendedores_{assign_id}.html")
    mapa.save(caminho)

    logger.success(f"🗺️ Mapa salvo em: {caminho}")
    return caminho


# =========================================================
# 4️⃣ Execução via CLI
# =========================================================
def main():
    parser = argparse.ArgumentParser(description="Gera mapa interativo de PDVs e bases de vendedores (por assign_id).")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--assign_id", type=str, required=True, help="Assign ID de referência (UUID)")
    args = parser.parse_args()

    logger.info(f"🗺️ Gerando mapa de vendedores | Tenant={args.tenant} | Assign={args.assign_id}")

    pdvs_df = buscar_pdvs_por_assign(args.tenant, args.assign_id)
    bases_df = buscar_bases_vendedores(args.tenant, args.assign_id)

    gerar_mapa_vendedores(pdvs_df, bases_df, args.tenant, args.assign_id)


if __name__ == "__main__":
    main()


#sales_router/src/sales_clusterization/visualization/cluster_plotting.py

# =========================================================
# 📦 src/sales_clusterization/visualization/cluster_plotting.py
# =========================================================

import folium
import argparse
import webbrowser
from pathlib import Path
from loguru import logger
from src.database.db_connection import get_connection


# =========================================================
# 1️⃣ BUSCAS DE DADOS
# =========================================================

def buscar_run_por_clusterization_id(tenant_id: int, clusterization_id: str):
    """
    Retorna o run_id correspondente a um clusterization_id e tenant_id.
    """
    sql = """
        SELECT id
        FROM cluster_run
        WHERE tenant_id = %s AND clusterization_id = %s
        LIMIT 1;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (tenant_id, clusterization_id))
        row = cur.fetchone()
    conn.close()
    return row[0] if row else None


def buscar_clusters(tenant_id: int, run_id: int):
    """
    Busca clusters (setores) e PDVs vinculados ao run_id informado.
    Se o pdv_endereco_completo estiver nulo em cluster_setor_pdv, reconstrói
    automaticamente a partir dos campos da tabela pdvs.
    """
    sql = """
        SELECT 
            cs.cluster_label,
            cs.centro_lat,
            cs.centro_lon,
            COALESCE(csp.lat, p.pdv_lat) AS lat,
            COALESCE(csp.lon, p.pdv_lon) AS lon,
            COALESCE(csp.cidade, p.cidade) AS cidade,
            COALESCE(csp.uf, p.uf) AS uf,
            COALESCE(
                NULLIF(csp.pdv_endereco_completo, ''),
                NULLIF(p.pdv_endereco_completo, ''),
                CONCAT(
                    COALESCE(p.logradouro, ''),
                    CASE WHEN p.numero IS NOT NULL AND p.numero <> '' THEN CONCAT(', ', p.numero) ELSE '' END,
                    CASE WHEN p.bairro IS NOT NULL AND p.bairro <> '' THEN CONCAT(' - ', p.bairro) ELSE '' END,
                    CASE WHEN p.cidade IS NOT NULL AND p.cidade <> '' THEN CONCAT(', ', p.cidade) ELSE '' END,
                    CASE WHEN p.uf IS NOT NULL AND p.uf <> '' THEN CONCAT('/', p.uf) ELSE '' END
                )
            ) AS pdv_endereco_completo,
            COALESCE(csp.cnpj, p.cnpj) AS cnpj
        FROM cluster_setor cs
        JOIN cluster_setor_pdv csp 
            ON csp.cluster_id = cs.id 
           AND csp.tenant_id = cs.tenant_id
        LEFT JOIN pdvs p 
            ON p.id = csp.pdv_id 
           AND p.tenant_id = csp.tenant_id
        WHERE cs.run_id = %s 
          AND cs.tenant_id = %s;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, tenant_id))
        rows = cur.fetchall()
    conn.close()
    return rows


# =========================================================
# 2️⃣ FUNÇÃO DE PLOTAGEM
# =========================================================

def gerar_mapa_clusters(dados, output_path: Path):
    """
    Gera mapa HTML com clusters (macrosetores) e PDVs colorindo por cluster_label.
    Cada PDV exibe CNPJ e endereço completo no popup e tooltip.
    """
    import pandas as pd
    import random
    import math

    if not dados:
        logger.warning("❌ Nenhum dado de clusterização encontrado.")
        return

    latitudes = [row[3] for row in dados if isinstance(row[3], (int, float)) and not math.isnan(row[3])]
    longitudes = [row[4] for row in dados if isinstance(row[4], (int, float)) and not math.isnan(row[4])]

    lat_centro = sum(latitudes) / len(latitudes) if latitudes else -15.78
    lon_centro = sum(longitudes) / len(longitudes) if longitudes else -47.93

    if pd.isna(lat_centro) or pd.isna(lon_centro):
        lat_centro, lon_centro = -15.78, -47.93  # fallback genérico (centro do Brasil)

    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=6, tiles="CartoDB positron")

    clusters = {}
    for row in dados:
        label, _, _, lat, lon, cidade, uf, endereco, cnpj = row
        if (
            lat is None or lon is None
            or not isinstance(lat, (int, float))
            or not isinstance(lon, (int, float))
            or math.isnan(lat) or math.isnan(lon)
            or math.isinf(lat) or math.isinf(lon)
        ):
            logger.debug(f"⚠️ Coordenadas inválidas ignoradas: Cluster {label} | {cidade}/{uf} | ({lat}, {lon})")
            continue
        clusters.setdefault(label, []).append((lat, lon, cidade, uf, endereco, cnpj))

    if not clusters:
        logger.warning("⚠️ Nenhum PDV válido encontrado para plotagem.")
        return

    random.seed(42)
    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
    ]

    for i, (label, pontos) in enumerate(sorted(clusters.items())):
        cor = palette[i % len(palette)]
        for lat, lon, cidade, uf, endereco, cnpj in pontos:
            # 🔧 Tratamento seguro de valores nulos
            endereco = endereco if endereco and endereco.strip() else "Endereço não informado"
            cidade = cidade if cidade and cidade.strip() else "Cidade não informada"
            uf = uf if uf and uf.strip() else "--"
            cnpj = cnpj if cnpj and cnpj.strip() else "CNPJ não informado"

            tooltip_text = endereco if endereco != "Endereço não informado" else f"{cidade}/{uf}"

            popup_html = f"""
            <b>Cluster:</b> {label}<br>
            <b>CNPJ:</b> {cnpj}<br>
            <b>Endereço:</b> {endereco}<br>
            <b>Cidade/UF:</b> {cidade}/{uf}<br>
            <b>Lat/Lon:</b> {lat:.6f}, {lon:.6f}
            """

            folium.CircleMarker(
                location=(lat, lon),
                radius=3,
                color=cor,
                fill=True,
                fill_opacity=0.85,
                popup=folium.Popup(popup_html, max_width=320),
                tooltip=folium.Tooltip(tooltip_text, sticky=True)
            ).add_to(m)

    legend_html = """
    <div style="
        position: fixed; bottom: 50px; left: 50px; width: 180px;
        z-index:9999; font-size:14px; background-color:white;
        border:2px solid grey; border-radius:8px; padding:10px;">
        <b>Clusters</b><br>{}
    </div>
    """.format("<br>".join([
        f"<span style='color:{palette[i % len(palette)]}'>●</span> Cluster {label}"
        for i, label in enumerate(sorted(clusters.keys()))
    ]))
    m.get_root().html.add_child(folium.Element(legend_html))

    if output_path.exists():
        output_path.unlink()

    m.save(output_path)
    logger.success(f"✅ Mapa de clusterização salvo em {output_path}")


# =========================================================
# 3️⃣ MAIN CLI
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Gerar mapa de clusterização de PDVs (multi-tenant, via clusterization_id)")
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant")
    parser.add_argument("--clusterization_id", type=str, required=True, help="UUID da clusterização a ser plotada")
    parser.add_argument("--modo_interativo", action="store_true", help="Abre o mapa no navegador (somente fora do Docker)")
    args = parser.parse_args()

    run_id = buscar_run_por_clusterization_id(args.tenant_id, args.clusterization_id)
    if not run_id:
        logger.error(f"❌ Nenhum run encontrado para tenant_id={args.tenant_id} e clusterization_id={args.clusterization_id}.")
        return

    logger.info(f"🗺️ Gerando mapa | tenant_id={args.tenant_id} | clusterization_id={args.clusterization_id} | run_id={run_id}")

    output_dir = Path(f"output/maps/{args.tenant_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    mapa_html = output_dir / f"clusterization_{args.clusterization_id}.html"

    dados = buscar_clusters(args.tenant_id, run_id)
    gerar_mapa_clusters(dados, mapa_html)

    if args.modo_interativo:
        try:
            webbrowser.open_new_tab(mapa_html.resolve().as_uri())
        except Exception as e:
            logger.warning(f"⚠️ Não foi possível abrir o navegador automaticamente: {e}")


if __name__ == "__main__":
    main()

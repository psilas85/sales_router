#sales_router/src/sales_clusterization/visualization/cluster_plotting.py

import folium
import argparse
import webbrowser
from pathlib import Path
from loguru import logger
from src.database.db_connection import get_connection


# =========================================================
# 1️⃣ BUSCAS DE DADOS
# =========================================================

def buscar_clusters(tenant_id: int, run_id: int):
    """
    Busca clusters (setores) e seus PDVs vinculados ao tenant e run_id.
    """
    sql = """
        SELECT cs.cluster_label, cs.centro_lat, cs.centro_lon,
               csp.lat, csp.lon, csp.cidade, csp.uf
        FROM cluster_setor cs
        JOIN cluster_setor_pdv csp ON csp.cluster_id = cs.id
        WHERE cs.run_id = %s AND cs.tenant_id = %s AND csp.tenant_id = %s;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (run_id, tenant_id, tenant_id))
        rows = cur.fetchall()
    conn.close()
    return rows


def buscar_ultimo_run(tenant_id: int):
    """
    Busca o último run finalizado do tenant informado.
    """
    sql = """
        SELECT id FROM cluster_run
        WHERE tenant_id = %s AND status = 'done'
        ORDER BY finished_at DESC
        LIMIT 1;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (tenant_id,))
        row = cur.fetchone()
    conn.close()
    return row[0] if row else None


# =========================================================
# 2️⃣ FUNÇÃO DE PLOTAGEM
# =========================================================

def gerar_mapa_clusters(dados, output_path: Path):
    """
    Gera mapa HTML com clusters (macrosetores) e PDVs, colorindo por cluster_label.
    """
    if not dados:
        logger.warning("❌ Nenhum dado de clusterização encontrado.")
        return

    # Centraliza o mapa com base nos PDVs
    latitudes = [row[3] for row in dados if row[3]]
    longitudes = [row[4] for row in dados if row[4]]
    lat_centro = sum(latitudes) / len(latitudes) if latitudes else -3.73
    lon_centro = sum(longitudes) / len(longitudes) if longitudes else -38.52
    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=9)

    # Agrupar por cluster_label
    clusters = {}
    for row in dados:
        label, centro_lat, centro_lon, lat, lon, cidade, uf = row
        clusters.setdefault(label, {"pdvs": [], "centro": (centro_lat, centro_lon)})
        clusters[label]["pdvs"].append((lat, lon, cidade, uf))

    # Paleta de cores legível
    import random
    random.seed(42)
    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
    ]

    for i, (label, info) in enumerate(sorted(clusters.items())):
        cor = palette[i % len(palette)]
        centro_lat, centro_lon = info["centro"]

        # Marcador do centro
        folium.Marker(
            location=(centro_lat, centro_lon),
            icon=folium.Icon(color="blue", icon="home"),
            popup=f"Centro Cluster {label}"
        ).add_to(m)

        # PDVs do cluster
        for lat, lon, cidade, uf in info["pdvs"]:
            folium.CircleMarker(
                location=(lat, lon),
                radius=3,
                color=cor,
                fill=True,
                fill_opacity=0.8,
                popup=f"Cluster {label} - {cidade}/{uf}"
            ).add_to(m)

    # Legenda
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

    # 🔁 Sempre sobrescreve o arquivo existente
    if output_path.exists():
        output_path.unlink()

    m.save(output_path)
    logger.success(f"✅ Mapa de clusterização salvo em {output_path}")


# =========================================================
# 3️⃣ MAIN CLI
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Gerar mapa de clusterização de PDVs (multi-tenant)")
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant")
    parser.add_argument("--run_id", type=int, help="ID da execução (opcional, busca último se omitido)")
    parser.add_argument("--modo_interativo", action="store_true", help="Abre o mapa no navegador (somente fora do Docker)")
    args = parser.parse_args()

    run_id = args.run_id or buscar_ultimo_run(args.tenant_id)
    if not run_id:
        logger.error(f"❌ Nenhum run_id encontrado para tenant_id={args.tenant_id}.")
        return

    logger.info(f"🗺️ Gerando mapa de clusterização | tenant_id={args.tenant_id} | run_id={run_id}")

    output_dir = Path(f"output/maps/{args.tenant_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # 🧩 Sobrescreve sempre o mesmo nome de arquivo
    mapa_html = output_dir / "clusterization_atual.html"

    dados = buscar_clusters(args.tenant_id, run_id)
    gerar_mapa_clusters(dados, mapa_html)

    # 🌐 Abre automaticamente (funciona apenas fora do container)
    if args.modo_interativo:
        if "DISPLAY" not in str(Path.home()):  # simples proteção contra ambiente sem GUI
            logger.info(f"🌐 Mapa disponível em: {mapa_html.resolve()}")
        try:
            webbrowser.open_new_tab(mapa_html.resolve().as_uri())
        except Exception as e:
            logger.warning(f"⚠️ Não foi possível abrir o navegador automaticamente: {e}")


if __name__ == "__main__":
    main()

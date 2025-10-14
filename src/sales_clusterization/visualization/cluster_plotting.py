#sales_router/src/sales_clusterization/visualization/cluster_plotting.py

import folium
import argparse
from pathlib import Path
from loguru import logger
from src.database.db_connection import get_connection


# =========================================================
# 1. BUSCAS DE DADOS
# =========================================================

def buscar_clusters(run_id: int):
    """
    Busca clusters (setores) e seus PDVs vinculados.
    """
    sql = """
        SELECT cs.cluster_label, cs.centro_lat, cs.centro_lon,
               csp.lat, csp.lon, csp.cidade, csp.uf
        FROM cluster_setor cs
        JOIN cluster_setor_pdv csp ON csp.cluster_id = cs.id
        WHERE cs.run_id = %s;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (run_id,))
        rows = cur.fetchall()
    conn.close()
    return rows


def buscar_ultimo_run():
    """
    Busca o último run finalizado na tabela cluster_run.
    """
    sql = """
        SELECT id FROM cluster_run
        WHERE status = 'done'
        ORDER BY finished_at DESC
        LIMIT 1;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql)
        row = cur.fetchone()
    conn.close()
    return row[0] if row else None


# =========================================================
# 2. FUNÇÃO DE PLOTAGEM
# =========================================================

def gerar_mapa_clusters(dados, output_path: Path):
    """
    Gera mapa HTML com clusters (macrosetores) e PDVs, colorindo por cluster_label.
    """
    if output_path.exists():
        output_path.unlink()  # sempre sobrescreve

    if not dados:
        logger.warning("❌ Nenhum dado de clusterização encontrado.")
        return

    m = folium.Map(location=[-3.73, -38.52], zoom_start=10)  # Centraliza em Fortaleza

    # Agrupar por cluster_label
    clusters = {}
    for row in dados:
        label, centro_lat, centro_lon, lat, lon, cidade, uf = row
        clusters.setdefault(label, {"pdvs": [], "centro": (centro_lat, centro_lon)})
        clusters[label]["pdvs"].append((lat, lon, cidade, uf))

    # Gerar cores fixas e legíveis
    import random
    random.seed(42)
    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf"
    ]

    for i, (label, info) in enumerate(sorted(clusters.items())):
        cor = palette[i % len(palette)]
        centro_lat, centro_lon = info["centro"]

        # Centro do cluster
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

    # Adiciona legenda simples
    legend_html = """
    <div style="
        position: fixed; 
        bottom: 50px; left: 50px; width: 180px; height: auto; 
        z-index:9999; font-size:14px; background-color:white;
        border:2px solid grey; border-radius:8px; padding:10px;">
        <b>Clusters</b><br>
        {}
    </div>
    """.format("<br>".join([f"<span style='color:{palette[i % len(palette)]}'>●</span> Cluster {label}"
                            for i, label in enumerate(sorted(clusters.keys()))]))

    m.get_root().html.add_child(folium.Element(legend_html))
    m.save(output_path)
    logger.success(f"✅ Mapa de clusterização salvo em {output_path}")



# =========================================================
# 3. MAIN CLI
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Gerar mapa de clusterização de PDVs")
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant")
    parser.add_argument("--run_id", type=int, help="ID da execução de clusterização (opcional)")
    args = parser.parse_args()

    run_id = args.run_id or buscar_ultimo_run()
    if not run_id:
        logger.error("❌ Nenhum run_id encontrado (nenhuma execução finalizada).")
        return

    logger.info(f"🗺️ Gerando mapa de clusterização para run_id={run_id} ...")

    output_dir = Path(f"output/maps/{args.tenant_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    # 🔁 Arquivo fixo para sobrescrever sempre o último mapa
    mapa_html = output_dir / "clusterization_atual.html"

    # Opcional: remover todos os anteriores para manter a pasta limpa
    for antigo in output_dir.glob("clusterization_*.html"):
        try:
            antigo.unlink()
        except Exception as e:
            logger.warning(f"Não foi possível remover {antigo}: {e}")

    dados = buscar_clusters(run_id)
    gerar_mapa_clusters(dados, mapa_html)



if __name__ == "__main__":
    main()

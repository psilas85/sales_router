#sales_router/src/sales_routing/visualization/route_plotting.py

import folium
import argparse
import json
import random
import matplotlib.pyplot as plt
from pathlib import Path
from statistics import mean
from loguru import logger
from folium import Map, CircleMarker, PolyLine
from branca.element import Template, MacroElement
from src.database.db_connection import get_connection


# =========================================================
# 1. Leitura do banco
# =========================================================
def buscar_rotas_operacionais(tenant_id: int):
    sql = """
        SELECT s.cluster_id, s.subcluster_seq, s.rota_coord, 
               p.lat, p.lon, p.sequencia_ordem
        FROM sales_subcluster s
        JOIN sales_subcluster_pdv p
          ON p.cluster_id = s.cluster_id 
         AND p.subcluster_seq = s.subcluster_seq
        WHERE s.tenant_id = %s
        ORDER BY s.cluster_id, s.subcluster_seq, p.sequencia_ordem;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (tenant_id,))
        rows = cur.fetchall()
        logger.info(f"📦 {len(rows)} registros de rota carregados para tenant {tenant_id}")
    conn.close()
    return rows


# =========================================================
# 2. Conversão segura do campo rota_coord
# =========================================================
def converter_rota_coord(rota_coord):
    try:
        if isinstance(rota_coord, (bytes, bytearray, memoryview)):
            rota_coord = rota_coord.tobytes().decode("utf-8")

        if isinstance(rota_coord, str):
            rota_coord = rota_coord.strip()
            if rota_coord.startswith("[") and "'" in rota_coord:
                rota_coord = rota_coord.replace("'", '"')
            rota_coord = json.loads(rota_coord)

        if isinstance(rota_coord, dict):
            rota_coord = [rota_coord]

        if not isinstance(rota_coord, list):
            return []

        coords = [
            (p.get("lat"), p.get("lon"))
            for p in rota_coord
            if isinstance(p, dict)
            and p.get("lat") is not None
            and p.get("lon") is not None
        ]
        return coords

    except Exception as e:
        logger.warning(f"⚠️ Falha ao decodificar rota_coord: {e}")
        return []


# =========================================================
# 3. Geração do mapa (visual idêntico ao HubRouter)
# =========================================================
def gerar_mapa_rotas(dados, output_path: Path, modo_debug: bool = False, zoom: int = 9):
    if output_path.exists():
        output_path.unlink()

    if not dados:
        logger.warning("❌ Nenhum dado de rota encontrado.")
        return

    rotas = {}
    todas_coords = []

    for cluster_id, sub_seq, rota_coord, lat, lon, ordem in dados:
        coords = converter_rota_coord(rota_coord)
        if lat is None or lon is None:
            continue
        rotas.setdefault((cluster_id, sub_seq), {"coord": coords, "pontos": []})
        rotas[(cluster_id, sub_seq)]["pontos"].append((lat, lon))
        todas_coords.extend(coords)

    if todas_coords:
        lat_centro = mean([c[0] for c in todas_coords])
        lon_centro = mean([c[1] for c in todas_coords])
    else:
        lat_centro, lon_centro = -15.78, -47.93

    # === Configuração idêntica ao HubRouter ===
    mapa = Map(
        location=[lat_centro, lon_centro],
        zoom_start=zoom,
        prefer_canvas=False,  # ⚙️ usa SVG renderer (mais suave)
        tiles="https://{s}.tile.openstreetmap.fr/hot/{z}/{x}/{y}.png",
        attr="© OpenStreetMap contributors"
    )

    # ============================================================
    # Cores e legenda por rota (subcluster)
    # ============================================================
    random.seed(42)
    cores = [f"#{random.randint(0, 0xFFFFFF):06x}" for _ in range(len(rotas))]

    # Mapeia cada rota (Cluster/Sub) → cor
    legenda_rotas = {}
    for idx, (rota_id) in enumerate(rotas.keys()):
        cluster_id, sub_seq = rota_id
        cor = cores[idx % len(cores)]
        legenda_rotas[f"Cluster {cluster_id} / Sub {sub_seq}"] = cor

    # === Legenda HTML por rota (subcluster) ===
    legenda_html = """
    {% macro html(this, kwargs) %}
    <div style="
        position: fixed;
        bottom: 40px;
        right: 40px;
        z-index: 9999;
        background-color: white;
        padding: 10px;
        border: 2px solid grey;
        border-radius: 6px;
        box-shadow: 2px 2px 6px rgba(0,0,0,0.3);
    ">
    <h4 style="margin-top: 0;">Legenda - Rotas</h4>
    <ul style="list-style: none; padding: 0; margin: 0;">
    """

    for nome, cor in legenda_rotas.items():
        legenda_html += f"""
        <li>
            <span style="background:{cor};width:12px;height:12px;display:inline-block;
            border-radius:50%;margin-right:6px;border:1px solid black;"></span>
            {nome}
        </li>
        """

    legenda_html += """
    </ul>
    </div>
    {% endmacro %}
    """

    legenda = MacroElement()
    legenda._template = Template(legenda_html)
    mapa.get_root().add_child(legenda)


    # === Desenha rotas ===
    for idx, ((cluster_id, sub_seq), info) in enumerate(rotas.items()):
        cor = cores[idx % len(cores)]
        coords = info["coord"]
        pontos_validos = [(lat, lon) for lat, lon in info["pontos"] if lat and lon]

        if modo_debug:
            logger.debug(f"\n🧭 Cluster {cluster_id} / Sub {sub_seq}")
            logger.debug(f"   - Pontos PDV: {len(pontos_validos)}")
            logger.debug(f"   - Pontos rota_coord: {len(coords)}")
            if coords:
                logger.debug(f"   ↳ Início: {coords[0]}")
                logger.debug(f"   ↳ Fim: {coords[-1]}")

        if len(coords) > 1:
            PolyLine(
                locations=coords,
                color=cor,
                weight=3,
                opacity=0.7,
                smooth_factor=1.0,  # 👈 igual ao HubRouter
                line_cap="round",
                line_join="round"
            ).add_to(mapa)
        elif pontos_validos:
            PolyLine(
                locations=pontos_validos,
                color=cor,
                weight=2,
                opacity=0.5,
                dash_array="5,5"
            ).add_to(mapa)

        # Pontos de entrega
        for lat, lon in pontos_validos:
            CircleMarker(
                location=(lat, lon),
                radius=3,
                color=cor,
                fill=True,
                fill_opacity=0.9
            ).add_to(mapa)

    mapa.save(output_path)
    logger.success(f"✅ Mapa HTML salvo: {output_path}")

    # === PNG (backup estático) ===
    png_path = str(output_path).replace(".html", ".png")
    plt.figure(figsize=(10, 8))
    for idx, ((cluster_id, sub_seq), info) in enumerate(rotas.items()):
        coords = info["coord"]
        if len(coords) > 1:
            lats, lons = zip(*coords)
            plt.plot(lons, lats, marker="o", linewidth=1.5,
                     color=cores[idx % len(cores)],
                     label=f"C{cluster_id}-S{sub_seq}")

    plt.xlabel("Longitude")
    plt.ylabel("Latitude")
    plt.title("Rotas Last-Mile (SalesRouter)")
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.savefig(png_path, dpi=150)
    plt.close()
    logger.info(f"🖼️ PNG salvo: {png_path}")


# =========================================================
# 4. Execução principal
# =========================================================
def main():
    parser = argparse.ArgumentParser(description="Gera mapa de rotas reais (vias) por tenant.")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--modo_debug", action="store_true", help="Exibe logs detalhados")
    parser.add_argument("--zoom", type=int, default=9, help="Define o nível de zoom inicial (padrão=9)")
    args = parser.parse_args()

    output_dir = Path(f"output/maps/{args.tenant}")
    output_dir.mkdir(parents=True, exist_ok=True)
    output_path = output_dir / "routing_operacional.html"

    logger.info(f"🗺️ Gerando mapa de rotas reais para tenant {args.tenant}... (modo_debug={args.modo_debug}, zoom={args.zoom})")
    dados = buscar_rotas_operacionais(args.tenant)
    gerar_mapa_rotas(dados, output_path, modo_debug=args.modo_debug, zoom=args.zoom)


if __name__ == "__main__":
    main()

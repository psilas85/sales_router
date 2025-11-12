# =========================================================
# 📦 src/pdv_preprocessing/visualization/pdv_plotting.py
# =========================================================

import folium
import argparse
import webbrowser
import math
import random
import pandas as pd
from pathlib import Path
from loguru import logger
from database.db_connection import get_connection


# =========================================================
# 1️⃣ BUSCA DE PDVs
# =========================================================
def buscar_pdvs(tenant_id: int, input_id: str, uf: str = None, cidade: str = None):
    """
    Busca PDVs do tenant/input_id, filtrando opcionalmente por UF ou cidade.
    """
    sql = """
        SELECT 
            cnpj,
            COALESCE(pdv_lat, 0) AS lat,
            COALESCE(pdv_lon, 0) AS lon,
            cidade,
            uf,
            COALESCE(NULLIF(pdv_endereco_completo, ''), 
                CONCAT(
                    COALESCE(logradouro, ''), 
                    CASE WHEN numero IS NOT NULL AND numero <> '' THEN CONCAT(', ', numero) ELSE '' END,
                    CASE WHEN bairro IS NOT NULL AND bairro <> '' THEN CONCAT(' - ', bairro) ELSE '' END,
                    CASE WHEN cidade IS NOT NULL AND cidade <> '' THEN CONCAT(', ', cidade) ELSE '' END,
                    CASE WHEN uf IS NOT NULL AND uf <> '' THEN CONCAT('/', uf) ELSE '' END
                )
            ) AS endereco
        FROM pdvs
        WHERE tenant_id = %s AND input_id = %s
    """

    params = [tenant_id, input_id]

    if cidade:
        sql += " AND LOWER(cidade) = LOWER(%s)"
        params.append(cidade)
    elif uf:
        sql += " AND LOWER(uf) = LOWER(%s)"
        params.append(uf)

    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, params)
        rows = cur.fetchall()
    conn.close()

    return rows


# =========================================================
# 2️⃣ PLOTAGEM
# =========================================================
def gerar_mapa_pdvs(dados, output_path: Path):
    """
    Gera mapa HTML com PDVs, colorindo por município.
    """
    if not dados:
        logger.warning("❌ Nenhum PDV encontrado para os filtros informados.")
        return

    # Converter para DataFrame
    df = pd.DataFrame(dados, columns=["cnpj", "lat", "lon", "cidade", "uf", "endereco"])

    # Filtra coordenadas válidas
    df = df[
        df["lat"].apply(lambda x: isinstance(x, (int, float)) and not math.isnan(x))
        & df["lon"].apply(lambda x: isinstance(x, (int, float)) and not math.isnan(x))
    ]

    if df.empty:
        logger.warning("⚠️ Nenhum PDV com coordenadas válidas para plotagem.")
        return

    # Centro do mapa
    lat_centro = df["lat"].mean()
    lon_centro = df["lon"].mean()

    m = folium.Map(location=[lat_centro, lon_centro], zoom_start=7, tiles="CartoDB positron")

    # Gera paleta fixa de cores
    random.seed(42)
    palette = [
        "#1f77b4", "#ff7f0e", "#2ca02c", "#d62728", "#9467bd",
        "#8c564b", "#e377c2", "#7f7f7f", "#bcbd22", "#17becf",
        "#0047AB", "#FFB347", "#00CED1", "#ADFF2F", "#CD5C5C"
    ]

    cidades = sorted(df["cidade"].dropna().unique())
    cores = {cidade: palette[i % len(palette)] for i, cidade in enumerate(cidades)}

    # Plotagem
    for _, row in df.iterrows():
        cidade = row["cidade"] or "Desconhecida"
        cor = cores.get(cidade, "#7f7f7f")

        popup_html = f"""
        <b>CNPJ:</b> {row['cnpj']}<br>
        <b>Endereço:</b> {row['endereco']}<br>
        <b>Cidade/UF:</b> {cidade}/{row['uf']}<br>
        <b>Lat/Lon:</b> {row['lat']:.6f}, {row['lon']:.6f}
        """

        folium.CircleMarker(
            location=(row["lat"], row["lon"]),
            radius=3,
            color=cor,
            fill=True,
            fill_opacity=0.85,
            popup=folium.Popup(popup_html, max_width=320),
            tooltip=folium.Tooltip(f"{cidade}/{row['uf']}", sticky=True),
        ).add_to(m)

    # Legenda
    legend_html = """
    <div style="
        position: fixed; bottom: 50px; left: 50px; width: 200px;
        z-index:9999; font-size:14px; background-color:white;
        border:2px solid grey; border-radius:8px; padding:10px;">
        <b>Cidades</b><br>{}
    </div>
    """.format("<br>".join([
        f"<span style='color:{cores[cidade]}'>●</span> {cidade}" for cidade in cidades
    ]))
    m.get_root().html.add_child(folium.Element(legend_html))

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_path.exists():
        output_path.unlink()

    m.save(output_path)
    logger.success(f"✅ Mapa de PDVs salvo em {output_path}")


# =========================================================
# 3️⃣ MAIN CLI
# =========================================================
def main():
    parser = argparse.ArgumentParser(description="Gerar mapa de PDVs (multi-tenant)")
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant")
    parser.add_argument("--input_id", type=str, required=True, help="UUID do input de PDVs")
    parser.add_argument("--uf", type=str, help="UF opcional para filtrar")
    parser.add_argument("--cidade", type=str, help="Cidade opcional para filtrar (prioritário)")
    parser.add_argument("--modo_interativo", action="store_true", help="Abre o mapa no navegador (fora do Docker)")
    args = parser.parse_args()

    logger.info(f"🗺️ Gerando mapa de PDVs | tenant={args.tenant_id} | input={args.input_id} | UF={args.uf or '--'} | Cidade={args.cidade or '--'}")

    dados = buscar_pdvs(args.tenant_id, args.input_id, args.uf, args.cidade)
    output_dir = Path(f"/app/output/maps/{args.tenant_id}")
    output_dir.mkdir(parents=True, exist_ok=True)

    nome_arquivo = f"pdvs_{args.input_id}_{args.cidade or args.uf or 'BR'}.html".replace(" ", "_")
    output_path = output_dir / nome_arquivo

    gerar_mapa_pdvs(dados, output_path)

    if args.modo_interativo:
        try:
            webbrowser.open_new_tab(output_path.resolve().as_uri())
        except Exception as e:
            logger.warning(f"⚠️ Não foi possível abrir o navegador: {e}")


if __name__ == "__main__":
    main()

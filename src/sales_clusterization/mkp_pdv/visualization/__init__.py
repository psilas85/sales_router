import folium
import pandas as pd
import argparse
from pathlib import Path
from loguru import logger
from src.database.db_connection import get_connection

def buscar(tenant_id, clusterization_id):
    sql = """
        SELECT pdv_id, cnpj, lat, lon, cluster_id,
               cluster_lat, cluster_lon, cluster_bairro,
               distancia_km, tempo_min
        FROM mkp_cluster_pdv
        WHERE tenant_id = %s
          AND clusterization_id = %s;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (tenant_id, clusterization_id))
        rows = cur.fetchall()
    conn.close()

    cols = [
        "pdv_id", "cnpj", "lat", "lon", "cluster_id",
        "cluster_lat", "cluster_lon", "cluster_bairro",
        "dist_km", "tempo_min"
    ]
    return pd.DataFrame(rows, columns=cols)

def plot(df, out_path):
    lat = df["lat"].mean()
    lon = df["lon"].mean()

    m = folium.Map(location=[lat, lon], zoom_start=11, tiles="CartoDB positron")

    for _, r in df.iterrows():
        folium.CircleMarker(
            location=[r.lat, r.lon],
            radius=6,
            color="blue",
            fill=True,
            popup=f"PDV {r.pdv_id}<br>CNPJ {r.cnpj}<br>Cluster {r.cluster_id}"
        ).add_to(m)

    m.save(out_path)
    logger.info(f"Mapa salvo em {out_path}")

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tenant_id", type=int, required=True)
    p.add_argument("--clusterization_id", type=str, required=True)
    args = p.parse_args()

    df = buscar(args.tenant_id, args.clusterization_id)
    output = Path(f"output/maps/{args.tenant_id}/mkp_pdv_{args.clusterization_id}.html")
    output.parent.mkdir(parents=True, exist_ok=True)

    plot(df, output)

if __name__ == "__main__":
    main()

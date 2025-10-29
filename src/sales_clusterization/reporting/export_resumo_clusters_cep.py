# ============================================================
# 📦 src/sales_clusterization/reporting/export_resumo_clusters_cep.py
# ============================================================

import os
import time
import pandas as pd
import argparse
import requests
from loguru import logger
from database.db_connection import get_connection


def obter_bairro_por_coord(lat, lon):
    """
    Faz reverse geocoding via Nominatim (local ou público)
    para retornar o bairro aproximado das coordenadas.
    Retorna None em caso de erro.
    """
    try:
        NOMINATIM_URL = os.getenv("NOMINATIM_URL", "http://nominatim:8080/reverse")
        params = {
            "lat": lat,
            "lon": lon,
            "format": "json",
            "addressdetails": 1,
            "zoom": 16
        }
        resp = requests.get(NOMINATIM_URL, params=params, timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            addr = data.get("address", {})
            return (
                addr.get("suburb")
                or addr.get("neighbourhood")
                or addr.get("city_district")
                or addr.get("village")
                or addr.get("town")
                or addr.get("city")
            )
        return None
    except Exception as e:
        logger.warning(f"⚠️ Falha ao buscar bairro para ({lat}, {lon}): {e}")
        return None


def exportar_resumo_clusters(tenant_id: int, clusterization_id: str):
    """
    Exporta resumo por cluster (clientes, CEPs, distâncias, tempos e bairro central)
    para CSV em output/reports/{tenant_id}/mkp_resumo_clusters_{clusterization_id}.csv
    """
    sql = """
        SELECT 
            tenant_id,
            clusterization_id,
            cluster_id,
            MIN(cluster_bairro) AS cluster_bairro,
            ROUND(AVG(cluster_lat)::numeric, 6) AS cluster_lat,
            ROUND(AVG(cluster_lon)::numeric, 6) AS cluster_lon,
            COUNT(DISTINCT cep) AS qtd_ceps,
            SUM(clientes_total) AS clientes_total,
            SUM(clientes_target) AS clientes_target,
            ROUND(AVG(distancia_km)::numeric, 2) AS distancia_media_km,
            ROUND(AVG(tempo_min)::numeric, 2) AS tempo_medio_min,
            ROUND(MAX(distancia_km)::numeric, 2) AS distancia_max_km,
            ROUND(MAX(tempo_min)::numeric, 2) AS tempo_max_min
        FROM mkp_cluster_cep
        WHERE tenant_id = %s AND clusterization_id = %s
        GROUP BY tenant_id, clusterization_id, cluster_id
        ORDER BY cluster_id;
    """

    conn = get_connection()
    df = pd.read_sql(sql, conn, params=(tenant_id, clusterization_id))
    conn.close()

    if df.empty:
        logger.warning(f"⚠️ Nenhum resumo encontrado para clusterization_id={clusterization_id}")
        return None

    # ============================================================
    # 🧮 Reverse geocoding apenas para cluster_bairro ausente
    # ============================================================
    logger.info("🌍 Obtendo bairros de cada centro de cluster via reverse geocoding...")
    bairros_cache = {}

    for i, row in df.iterrows():
        lat, lon = float(row["cluster_lat"]), float(row["cluster_lon"])
        chave = f"{lat:.6f},{lon:.6f}"

        if pd.isna(row["cluster_bairro"]) or not str(row["cluster_bairro"]).strip():
            if chave in bairros_cache:
                bairro = bairros_cache[chave]
            else:
                bairro = obter_bairro_por_coord(lat, lon)
                bairros_cache[chave] = bairro
                time.sleep(0.3)  # evita flood no Nominatim

            df.at[i, "cluster_bairro"] = bairro or "Não identificado"

    # ============================================================
    # 📏 Formata apenas latitude/longitude com vírgula
    # ============================================================
    df["cluster_lat"] = df["cluster_lat"].map(lambda x: f"{x:.6f}".replace(".", ","))
    df["cluster_lon"] = df["cluster_lon"].map(lambda x: f"{x:.6f}".replace(".", ","))

    # Todas as demais colunas numéricas mantêm ponto decimal padrão
    numeric_cols = [
        "qtd_ceps", "clientes_total", "clientes_target",
        "distancia_media_km", "tempo_medio_min",
        "distancia_max_km", "tempo_max_min"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # ============================================================
    # 📊 Preview no log
    # ============================================================
    logger.info("📋 Prévia dos clusters exportados:")
    logger.info(df[["cluster_id", "cluster_bairro", "cluster_lat", "cluster_lon"]].head(10).to_string(index=False))

    # ============================================================
    # 💾 Exporta CSV formatado
    # ============================================================
    output_dir = os.path.join("output", "reports", str(tenant_id))
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"mkp_resumo_clusters_{clusterization_id}.csv")

    df.to_csv(
        output_path,
        index=False,
        sep=";",
        encoding="utf-8-sig",
        float_format="%.2f"
    )

    logger.success(f"✅ Resumo de clusters exportado para {output_path}")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exportar resumo de clusterização MKP para CSV")
    parser.add_argument("--tenant_id", required=True, type=int, help="Tenant ID")
    parser.add_argument("--clusterization_id", required=True, help="Clusterization ID")
    args = parser.parse_args()

    exportar_resumo_clusters(args.tenant_id, args.clusterization_id)

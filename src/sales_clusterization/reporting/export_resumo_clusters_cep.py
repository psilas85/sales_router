# ============================================================
# 📦 src/sales_clusterization/reporting/export_resumo_clusters_cep.py
# ============================================================

import os
import time
import random
import pandas as pd
import argparse
import requests
from loguru import logger
from database.db_connection import get_connection


def obter_bairro_por_coord(lat, lon):
    """
    Obtém o bairro aproximado via Nominatim público com fallback no Google Maps.
    Retorna string com nome do bairro ou 'Não identificado'.
    """
    try:
        NOMINATIM_URL = os.getenv("NOMINATIM_URL", "https://nominatim.openstreetmap.org")
        params = {"lat": lat, "lon": lon, "format": "json", "addressdetails": 1, "zoom": 16}
        headers = {
            "User-Agent": "SalesRouter/1.0 (suporte@salesrouter.ai)",
            "Accept-Language": "pt-BR",
        }

        resp = requests.get(f"{NOMINATIM_URL}/reverse", params=params, headers=headers, timeout=10)

        if resp.status_code == 200:
            data = resp.json()
            addr = data.get("address", {})
            bairro = (
                addr.get("suburb")
                or addr.get("neighbourhood")
                or addr.get("city_district")
                or addr.get("village")
                or addr.get("town")
                or addr.get("city")
            )
            if bairro:
                logger.debug(f"🏙️ Bairro via Nominatim ({lat},{lon}) → {bairro}")
                return bairro.strip()
        else:
            logger.warning(f"⚠️ Nominatim {resp.status_code} para ({lat},{lon})")
    except Exception as e:
        logger.warning(f"⚠️ Erro Nominatim ({lat},{lon}): {e}")

    # fallback Google
    try:
        GOOGLE_KEY = os.getenv("GMAPS_API_KEY")
        if not GOOGLE_KEY:
            return "Não identificado"

        google_url = (
            f"https://maps.googleapis.com/maps/api/geocode/json?"
            f"latlng={lat},{lon}&key={GOOGLE_KEY}&language=pt-BR"
        )
        g_resp = requests.get(google_url, timeout=10)
        if g_resp.status_code == 200:
            g_data = g_resp.json()
            if g_data.get("results"):
                for comp in g_data["results"][0].get("address_components", []):
                    if "sublocality" in comp["types"] or "neighborhood" in comp["types"]:
                        return comp["long_name"].strip()
        return "Não identificado"
    except Exception as e:
        logger.warning(f"⚠️ Erro Google Geocoding ({lat},{lon}): {e}")
        return "Não identificado"


def formatar_cnpj(cnpj_raw: str) -> str:
    """Formata CNPJ corretamente, mesmo se vier em float, notação científica ou com vírgula."""
    try:
        cnpj_str = str(cnpj_raw).strip().replace(",", ".")
        # Se vier em notação científica (ex: 2.17E+13)
        if "E" in cnpj_str.upper():
            cnpj_str = "{:.0f}".format(float(cnpj_str))
        # Extrai apenas dígitos
        cnpj_digits = "".join(filter(str.isdigit, cnpj_str))
        # Preenche com zeros à esquerda, se vier menor
        if len(cnpj_digits) < 14:
            cnpj_digits = cnpj_digits.zfill(14)
        if len(cnpj_digits) == 14:
            return f"{cnpj_digits[:2]}.{cnpj_digits[2:5]}.{cnpj_digits[5:8]}/{cnpj_digits[8:12]}-{cnpj_digits[12:]}"
        return cnpj_digits

    except Exception:
        return str(cnpj_raw).strip()



def exportar_resumo_clusters(tenant_id: int, clusterization_id: str):
    """
    Exporta resumo por cluster (clientes, CEPs, distâncias, tempos e bairro central)
    """
    sql = """
        SELECT 
            tenant_id,
            clusterization_id,
            cluster_id,
            MIN(centro_nome) AS centro_nome,
            MIN(centro_cnpj) AS centro_cnpj,
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

    logger.info("🌍 Obtendo bairros de cada centro de cluster via reverse geocoding...")
    bairros_cache = {}

    for i, row in df.iterrows():
        try:
            lat, lon = float(row["cluster_lat"]), float(row["cluster_lon"])
            chave = f"{lat:.6f},{lon:.6f}"
            if pd.isna(row["cluster_bairro"]) or not str(row["cluster_bairro"]).strip():
                if chave in bairros_cache:
                    bairro = bairros_cache[chave]
                else:
                    bairro = obter_bairro_por_coord(lat, lon)
                    bairros_cache[chave] = bairro
                df.at[i, "cluster_bairro"] = bairro or "Não identificado"
        except Exception as e:
            logger.warning(f"⚠️ Falha ao processar linha {i}: {e}")
            df.at[i, "cluster_bairro"] = "Não identificado"

    # 🧾 formatações
    df["centro_cnpj"] = (
        df["centro_cnpj"]
        .astype(str)
        .apply(lambda x: formatar_cnpj(x))
    )
    df["cluster_lat"] = df["cluster_lat"].map(lambda x: f"{x:.6f}".replace(".", ","))
    df["cluster_lon"] = df["cluster_lon"].map(lambda x: f"{x:.6f}".replace(".", ","))

    numeric_cols = [
        "qtd_ceps", "clientes_total", "clientes_target",
        "distancia_media_km", "tempo_medio_min",
        "distancia_max_km", "tempo_max_min"
    ]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    logger.info("📋 Prévia dos clusters exportados:")
    logger.info(
        df[["cluster_id", "centro_nome", "centro_cnpj", "cluster_bairro", "cluster_lat", "cluster_lon"]]
        .head(10)
        .to_string(index=False)
    )

    output_dir = os.path.join("output", "reports", str(tenant_id))
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(output_dir, f"mkp_resumo_clusters_{clusterization_id}.csv")

    df.to_csv(output_path, index=False, sep=";", encoding="utf-8-sig", float_format="%.2f")
    logger.success(f"✅ Resumo de clusters exportado para {output_path}")
    return output_path


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exportar resumo de clusterização MKP para CSV")
    parser.add_argument("--tenant_id", required=True, type=int, help="Tenant ID")
    parser.add_argument("--clusterization_id", required=True, help="Clusterization ID")
    args = parser.parse_args()

    exportar_resumo_clusters(args.tenant_id, args.clusterization_id)

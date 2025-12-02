# sales_router/src/sales_clusterization/mkp_pdv/domain/centers_loader.py

import pandas as pd
from loguru import logger
from typing import List, Dict
from src.database.db_connection import get_connection
from pdv_preprocessing.domain.utils_geo import coordenada_generica

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
import requests
import os


# ============================================================
# 🔹 CACHE
# ============================================================
def buscar_cache(endereco: str):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            SELECT lat, lon
            FROM enderecos_cache
            WHERE endereco = %s
            LIMIT 1
            """,
            (endereco,)
        )
        row = cur.fetchone()
        cur.close()
        conn.close()

        if row and row[0] and row[1]:
            return float(row[0]), float(row[1])

    except Exception as e:
        logger.warning(f"⚠️ Erro lendo cache: {e}")

    return None


def salvar_cache(endereco, lat, lon, origem):
    try:
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO enderecos_cache (endereco, lat, lon, origem)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (endereco) DO NOTHING
            """,
            (endereco, lat, lon, origem)
        )
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"❌ Falha salvando no cache: {e}")


# ============================================================
# 🔹 NOMINATIM
# ============================================================
def geocode_nominatim(endereco: str):
    try:
        params = {
            "q": endereco,
            "format": "json",
            "countrycodes": "br",
            "addressdetails": 1,
        }
        headers = {"User-Agent": "SalesRouter-Geocoder/1.0"}

        r = requests.get(NOMINATIM_URL, params=params, headers=headers, timeout=7)

        if r.status_code == 200 and r.json():
            item = r.json()[0]
            return float(item["lat"]), float(item["lon"])
    except Exception as e:
        logger.warning(f"⚠️ Erro Nominatim: {e}")

    return None


# ============================================================
# 🔹 GOOGLE
# ============================================================
def geocode_google(endereco: str):
    key = os.getenv("GMAPS_API_KEY")
    if not key:
        return None

    try:
        url = (
            "https://maps.googleapis.com/maps/api/geocode/json?"
            f"address={requests.utils.quote(endereco)}&key={key}"
        )
        dados = requests.get(url, timeout=7).json()

        if dados.get("status") == "OK":
            loc = dados["results"][0]["geometry"]["location"]
            return float(loc["lat"]), float(loc["lng"])
    except Exception as e:
        logger.warning(f"⚠️ Erro Google: {e}")

    return None


# ============================================================
# 🔥 GEOCODIFICAR UM ÚNICO ENDEREÇO COMPLETO
# ============================================================
def geocodificar_um(endereco: str):
    if not endereco:
        return None, None, "invalido"

    endereco = endereco.strip()

    # CACHE
    res = buscar_cache(endereco)
    if res:
        lat, lon = res
        if not coordenada_generica(lat, lon):
            return lat, lon, "cache"

    # NOMINATIM
    res = geocode_nominatim(endereco)
    if res:
        lat, lon = res
        if not coordenada_generica(lat, lon):
            salvar_cache(endereco, lat, lon, "nominatim")
            return lat, lon, "nominatim"

    # GOOGLE
    res = geocode_google(endereco)
    if res:
        lat, lon = res
        if not coordenada_generica(lat, lon):
            salvar_cache(endereco, lat, lon, "google")
            return lat, lon, "google"

    return None, None, "falha"


# ============================================================
# 🔥 LOADER COMPLETO — CSV → CENTROS GEOCODIFICADOS
# ============================================================
class CentersLoader:

    @staticmethod
    def carregar_centros(csv_path: str) -> List[Dict]:
        logger.info(f"📥 Lendo CSV de centros: {csv_path}")

        df = pd.read_csv(csv_path, sep=";", encoding="utf-8")
        df.columns = df.columns.str.lower().str.strip()

        # Normalização dos nomes das colunas
        df = df.rename(columns={
            "bandeira cliente": "bandeira",
            "rua_numero": "rua_numero",
            "cidade": "cidade",
            "bairro": "bairro",
            "uf": "uf",
        })

        obrigatorias = {"rua_numero", "bairro", "cidade", "uf"}
        if not obrigatorias.issubset(df.columns):
            raise Exception(f"CSV deve conter colunas obrigatórias: {obrigatorias}")

        # Monta endereço completo
        df["endereco_fmt"] = (
            df["rua_numero"].astype(str).str.strip() + ", " +
            df["bairro"].astype(str).str.strip() + ", " +
            df["cidade"].astype(str).str.strip() + " - " +
            df["uf"].astype(str).str.strip() + ", Brasil"
        )

        logger.info(f"📥 Centros carregados: {len(df)} (pré-geocodificação)")

        return CentersLoader.geocodificar_centros(df)

    @staticmethod
    def geocodificar_centros(df):
        resultados = []

        for _, row in df.iterrows():
            endereco = row["endereco_fmt"]
            lat, lon, origem = geocodificar_um(endereco)

            resultados.append({
                "cluster_id": int(row["cluster_id"]),
                "bandeira": row.get("bandeira"),
                "cliente": row.get("cliente"),
                "cnpj": row.get("cnpj"),
                "endereco": endereco,
                "lat": lat,
                "lon": lon,
                "origem": origem,
                "bairro": row.get("bairro"),
                "cidade": row.get("cidade"),
                "uf": row.get("uf"),
            })

        logger.info(f"🏗️ {len(resultados)} centros geocodificados.")
        return resultados

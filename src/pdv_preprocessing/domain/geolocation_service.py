# src/pdv_preprocessing/domain/geolocation_service.py

import os
import time
import requests
import logging
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter


class GeolocationService:
    def __init__(self, reader: DatabaseReader, writer: DatabaseWriter):
        self.reader = reader
        self.writer = writer
        self.geolocator = Nominatim(user_agent="sales_router_pdv")
        self.GOOGLE_KEY = os.getenv("GMAPS_API_KEY")
        self.cache_mem = {}

        # Contadores de desempenho
        self.stats = {
            "cache_mem": 0,
            "cache_db": 0,
            "nominatim": 0,
            "google": 0,
            "falha": 0,
            "total": 0,
        }

    # ===============================================================
    # Busca principal (cache → Nominatim → Google)
    # ===============================================================
    def buscar_coordenadas(self, endereco: str, uf: str = None):
        if not endereco:
            self.stats["falha"] += 1
            return None, None, "endereco_vazio"

        endereco_norm = endereco.strip().lower()
        self.stats["total"] += 1

        # 1️⃣ Cache em memória
        if endereco_norm in self.cache_mem:
            self.stats["cache_mem"] += 1
            logging.info(f"🧠 [CACHE_MEM] {endereco_norm}")
            lat, lon = self.cache_mem[endereco_norm]
            return lat, lon, "cache_mem"

        # 2️⃣ Cache no banco
        inicio = time.time()
        cache_db = self.reader.buscar_localizacao(endereco_norm)
        if cache_db:
            dur = time.time() - inicio
            lat, lon = cache_db
            self.cache_mem[endereco_norm] = (lat, lon)
            self.stats["cache_db"] += 1
            logging.info(f"🗄️ [CACHE_DB] ({dur:.2f}s) {endereco_norm} → ({lat}, {lon})")
            return lat, lon, "cache_db"

        # 3️⃣ Nominatim (tentativa 1)
        inicio = time.time()
        coords = self._buscar_nominatim(endereco_norm)
        dur = time.time() - inicio
        if coords:
            lat, lon = coords
            self.writer.inserir_localizacao(endereco_norm, lat, lon)
            self.cache_mem[endereco_norm] = coords
            self.stats["nominatim"] += 1
            logging.info(f"🧭 [NOMINATIM] ({dur:.2f}s) {endereco_norm} → ({lat}, {lon})")
            return lat, lon, "nominatim"
        else:
            logging.warning(f"⚠️ [NOMINATIM] Falha ({dur:.2f}s) para {endereco_norm}")

        # 4️⃣ Google fallback
        inicio = time.time()
        coords = self._buscar_google(endereco_norm)
        dur = time.time() - inicio
        if coords:
            lat, lon = coords
            self.writer.inserir_localizacao(endereco_norm, lat, lon)
            self.cache_mem[endereco_norm] = coords
            self.stats["google"] += 1
            logging.info(f"🌍 [GOOGLE] ({dur:.2f}s) {endereco_norm} → ({lat}, {lon})")
            return lat, lon, "google"
        else:
            logging.warning(f"⚠️ [GOOGLE] Falha ({dur:.2f}s) para {endereco_norm}")

        # 5️⃣ Falha geral
        self.stats["falha"] += 1
        logging.error(f"💀 [FALHA] Nenhuma coordenada encontrada para {endereco_norm}")
        return None, None, "falha"

    # ===============================================================
    # Nominatim (consulta pública)
    # ===============================================================
    def _buscar_nominatim(self, endereco):
        try:
            response = requests.get(
                "https://nominatim.openstreetmap.org/search",
                params={"q": endereco, "format": "json", "limit": 1},
                headers={"User-Agent": "sales_router_pdv"},
                timeout=25,
            )
            response.raise_for_status()
            data = response.json()
            if data:
                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])
                time.sleep(1)  # respeita política de uso
                return lat, lon
        except (GeocoderTimedOut, Exception) as e:
            logging.warning(f"⚠️ [NOMINATIM] Erro: {e}")
        return None

    # ===============================================================
    # Google Maps (fallback)
    # ===============================================================
    def _buscar_google(self, endereco):
        if not self.GOOGLE_KEY:
            logging.warning("⚠️ [GOOGLE] Chave API não configurada (GMAPS_API_KEY ausente).")
            return None
        try:
            from urllib.parse import quote
            url = f"https://maps.googleapis.com/maps/api/geocode/json?address={quote(endereco)}&key={self.GOOGLE_KEY}"
            response = requests.get(url, timeout=15)
            response.raise_for_status()
            data = response.json()
            if data.get("status") == "OK":
                loc = data["results"][0]["geometry"]["location"]
                return loc["lat"], loc["lng"]
            else:
                logging.warning(f"⚠️ [GOOGLE] Status {data.get('status')} para {endereco}")
        except Exception as e:
            logging.warning(f"⚠️ [GOOGLE] Erro: {e}")
        return None

    # ===============================================================
    # Resumo final
    # ===============================================================
    def exibir_resumo_logs(self):
        logging.info("📊 Resumo de Geolocalização:")
        for origem, count in self.stats.items():
            if origem != "total":
                logging.info(f"   {origem:<10}: {count}")
        logging.info(f"   total      : {self.stats['total']}")

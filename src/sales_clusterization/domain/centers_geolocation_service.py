#sales_router/src/sales_clusterization/domain/centers_geolocation_service.py

import requests
from loguru import logger
from pdv_preprocessing.domain.utils_geo import coordenada_generica


class CentersGeolocationService:

    def __init__(self, reader, writer, google_key: str = None, timeout=7):
        self.reader = reader
        self.writer = writer
        self.google_key = google_key
        self.timeout = timeout
        self.NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"

    # ============================================================
    # 🔎 Busca principal
    # ============================================================
    def buscar(self, endereco: str):
        if not endereco or not str(endereco).strip():
            return None, None, "invalido"

        endereco = endereco.strip()

        # 1) CACHE
        lat, lon = self._buscar_cache(endereco)
        if lat and lon and not coordenada_generica(lat, lon):
            return lat, lon, "cache"

        # 2) NOMINATIM
        lat, lon = self._buscar_nominatim(endereco)
        if lat and lon and not coordenada_generica(lat, lon):
            self._salvar_cache(endereco, lat, lon, origem="nominatim")
            return lat, lon, "nominatim"

        # 3) GOOGLE
        if self.google_key:
            lat, lon = self._buscar_google(endereco)
            if lat and lon and not coordenada_generica(lat, lon):
                self._salvar_cache(endereco, lat, lon, origem="google")
                return lat, lon, "google"

        # 4) FALHA
        return None, None, "falha"

    # ============================================================
    # 📦 Cache
    # ============================================================
    def _buscar_cache(self, endereco):
        try:
            res = self.reader.buscar_endereco_cache(endereco)
            if res:
                return res["lat"], res["lon"]
        except Exception as e:
            logger.warning(f"⚠️ Erro lendo cache: {e}")
        return None, None

    def _salvar_cache(self, endereco, lat, lon, origem):
        try:
            self.writer.salvar_cache(endereco, lat, lon, origem)
        except Exception as e:
            logger.error(f"❌ Falha salvando no cache: {e}")

    # ============================================================
    # 🌍 NOMINATIM
    # ============================================================
    def _buscar_nominatim(self, endereco):
        try:
            params = {
                "q": endereco,
                "format": "json",
                "countrycodes": "br",
                "addressdetails": 1,
            }
            headers = {"User-Agent": "SalesRouter-Geocoder/1.0"}

            r = requests.get(self.NOMINATIM_URL, params=params, headers=headers, timeout=self.timeout)

            if r.status_code == 200 and r.json():
                item = r.json()[0]
                return float(item["lat"]), float(item["lon"])
        except Exception as e:
            logger.warning(f"⚠️ Erro Nominatim: {e}")

        return None, None

    # ============================================================
    # 🗺️ GOOGLE GEOCODING
    # ============================================================
    def _buscar_google(self, endereco):
        try:
            url = (
                "https://maps.googleapis.com/maps/api/geocode/json?"
                f"address={requests.utils.quote(endereco)}&key={self.google_key}"
            )
            dados = requests.get(url, timeout=self.timeout).json()

            if dados.get("status") == "OK":
                loc = dados["results"][0]["geometry"]["location"]
                return loc["lat"], loc["lng"]

        except Exception as e:
            logger.warning(f"⚠️ Erro Google: {e}")

        return None, None

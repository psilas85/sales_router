#sales_router/src/pdv_preprocessing/cep_area_geocoding/domain/area_geolocation_service.py
# ============================================================
# 📍 src/pdv_preprocessing/cep_area_geocoding/domain/area_geolocation_service.py
# ============================================================

import os
import time
import requests
from random import uniform
from loguru import logger
from pdv_preprocessing.domain.utils_geo import coordenada_generica


class AreaGeolocationService:

    def __init__(self, reader, writer):
        self.reader = reader
        self.writer = writer
        self.cache_mem = {}  # { cep : (lat, lon) }

    # --------------------------------------------------------
    def buscar(self, tenant_id, cep, endereco_key, bairro, cidade, uf, input_id=None):
        cep = str(cep).strip()

        # =====================================================
        # 1) CACHE MEMÓRIA
        # =====================================================
        if cep in self.cache_mem:
            lat, lon = self.cache_mem[cep]
            logger.info(f"⚡ cache_mem | CEP={cep} | input_id={input_id}")
            return lat, lon, "cache_mem"

        # =====================================================
        # 2) CACHE DB
        # =====================================================
        row = self.reader.buscar_cep_bairro_cache(tenant_id, cep)
        if row:
            lat, lon, origem_db = row
            self.cache_mem[cep] = (lat, lon)
            logger.info(f"🗄️ cache_db | CEP={cep} | origem={origem_db} | input_id={input_id}")
            return lat, lon, origem_db

        # =====================================================
        # 3) PHOTON (PRIMEIRA TENTATIVA)
        # =====================================================
        lat, lon = self._buscar_photon(endereco_key)

        if self._coord_valida(lat, lon):
            logger.info(
                f"📡 photon_ok | CEP={cep} | latlon=({lat}, {lon}) | input_id={input_id}"
            )
            return lat, lon, "photon"

        logger.warning(f"⚠️ photon_fail_full | CEP={cep} | input_id={input_id}")

        # =====================================================
        # PHOTON RETRY — endereço reduzido (cidade + UF)
        # reduz erros e aumenta taxa de acerto para grandes volumes
        # =====================================================
        endereco_reduzido = f"{cidade} - {uf}, Brasil"
        lat, lon = self._buscar_photon(endereco_reduzido)

        if self._coord_valida(lat, lon):
            logger.info(
                f"📡 photon_retry_ok | CEP={cep} | latlon=({lat}, {lon}) | input_id={input_id}"
            )
            return lat, lon, "photon_retry"

        logger.warning(f"⚠️ photon_retry_fail | CEP={cep} | input_id={input_id}")

        # =====================================================
        # 4) GOOGLE (fallback premium)
        # =====================================================
        lat, lon = self._buscar_google(endereco_key)

        if self._coord_valida(lat, lon):
            logger.info(
                f"🌍 google_ok | CEP={cep} | latlon=({lat}, {lon}) | input_id={input_id}"
            )
            return lat, lon, "google"

        logger.error(f"❌ geo_fail | CEP={cep} | endereco={endereco_key} | input_id={input_id}")
        return None, None, "fail"

    # =========================================================
    # PHOTON com rate-limit + validação de bounding-box
    # =========================================================
    def _buscar_photon(self, endereco):
        logger.info(f"🔎 [Photon] {endereco}")

        # anti-ban (40–120 ms)
        time.sleep(uniform(0.04, 0.12))

        try:
            r = requests.get(
                "https://photon.komoot.io/api/",
                params={"q": endereco},
                timeout=6,
                headers={"User-Agent": "SalesRouter-Geocoder"}
            )

            data = r.json()

            if not data.get("features"):
                return None, None

            lon, lat = data["features"][0]["geometry"]["coordinates"]
            lat, lon = float(lat), float(lon)

            return lat, lon

        except Exception as e:
            logger.error(f"❌ Photon error: {e}")
            return None, None

    # =========================================================
    # GOOGLE FALLBACK
    # =========================================================
    def _buscar_google(self, endereco):
        logger.info(f"🔎 [Google] {endereco}")

        api_key = os.getenv("GMAPS_API_KEY")
        if not api_key:
            logger.error("❌ GMAPS_API_KEY não definida")
            return None, None

        try:
            url = "https://maps.googleapis.com/maps/api/geocode/json"
            params = {
                "address": endereco,
                "key": api_key,
                "language": "pt-BR",
                "region": "br"
            }

            r = requests.get(url, params=params, timeout=10)
            data = r.json()

            status = data.get("status")

            if status == "ZERO_RESULTS":
                logger.warning(f"⚠️ Google ZERO_RESULTS → {endereco}")
                return None, None

            if status != "OK":
                logger.error(f"❌ Google API error ({status}): {data}")
                return None, None

            result = data["results"][0]["geometry"]["location"]

            return float(result["lat"]), float(result["lng"])

        except Exception as e:
            logger.error(f"❌ Google error: {e}")
            return None, None

    # =========================================================
    # FILTRO DE COORDENADA
    # =========================================================
    def _coord_valida(self, lat, lon):
        """
        Rejeita coords inválidas, genéricas e completamente fora de SP.
        Filtro para evitar lat/lon de Limeira, Sorocaba, Goiás, Sul etc.
        """
        if not lat or not lon:
            return False

        if coordenada_generica(lat, lon):
            return False

        # bounding-box São Paulo + entorno metropolitano
        if not (-25 <= lat <= -22 and -47.5 <= lon <= -45):
            logger.warning(f"⚠️ bounding_box_fail | latlon=({lat},{lon})")
            return False

        return True

#sales_router/src/geocoding_engine/services/geocoding_service.py

import os
import time
import requests
import threading
import re

from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

from geocoding_engine.domain.address_normalizer import (
    normalize_base,
    normalize_for_cache,
    normalize_for_geocoding,
)
from geocoding_engine.domain.capital_polygon_validator import (
    ponto_dentro_capital,
)
from geocoding_engine.infrastructure.nominatim_client import NominatimClient
from geocoding_engine.infrastructure.google_client import GoogleClient


class GeolocationService:
    """
    Motor puro de geocoding.

    Responsabilidades:
      - normalizar endereço
      - consultar cache global
      - consultar Nominatim estruturado
      - consultar Nominatim livre
      - fallback Google
      - retornar resultado

    NÃO persiste cache.
    NÃO persiste histórico.
    """

    def __init__(self, reader, max_workers=20):
        self.reader = reader

        self.nominatim = NominatimClient()
        self.google = GoogleClient()

        self.timeout = 5
        self.max_workers = max_workers

        self.GOOGLE_ENABLED = os.getenv(
            "ENABLE_GOOGLE_GEOCODING", "true"
        ).lower() == "true"

        self.stats = {
            "cache_db": 0,
            "nominatim_struct": 0,
            "nominatim_free": 0,
            "google": 0,
            "falha": 0,
            "total": 0,
        }

        self.stats_lock = threading.Lock()

    # ---------------------------------------------------------
    # Extração robusta cidade/UF
    # ---------------------------------------------------------

    def _extrair_cidade_uf(self, endereco):
        if not endereco:
            return None, None

        m = re.search(r"([^,]+)\s*-\s*([A-Z]{2})", endereco)
        if m:
            return m.group(1).strip().upper(), m.group(2).strip().upper()

        m = re.search(r"([^,]+),\s*([A-Z]{2})", endereco)
        if m:
            return m.group(1).strip().upper(), m.group(2).strip().upper()

        m = re.search(r"([A-Za-zÀ-ÿ\s]+)\s+([A-Z]{2})$", endereco)
        if m:
            return m.group(1).strip().upper(), m.group(2).strip().upper()

        return None, None

    # ---------------------------------------------------------
    # Coordenadas genéricas
    # ---------------------------------------------------------

    @staticmethod
    def _is_generic_location(lat, lon):
        pontos = [
            (-23.5506507, -46.6333824),  # Sé - SP
            (-22.908333, -43.196388),    # Centro - RJ
            (-15.7801, -47.9292),        # Brasília
            (-19.9167, -43.9345),        # BH
        ]

        for rlat, rlon in pontos:
            if abs(lat - rlat) < 0.0005 and abs(lon - rlon) < 0.0005:
                return True

        return False

    # ---------------------------------------------------------
    # Nominatim estruturado
    # ---------------------------------------------------------

    def _buscar_nominatim_estruturado(self, endereco):
        m = re.search(r",\s*([^,]+?)\s*-\s*([A-Za-z]{2})", endereco)

        if not m:
            return None

        city = m.group(1).strip()
        state = m.group(2).strip()

        street_part = endereco[:m.start()].strip().strip(",")

        if not street_part:
            return None

        street = street_part.split(",")[0].strip()

        headers = {"User-Agent": "SalesRouter-Geocoder"}

        params = {
            "street": street,
            "city": city,
            "state": state,
            "country": "Brazil",
            "format": "json",
            "limit": 1,
            "addressdetails": 1,
        }

        try:
            r = requests.get(
                f"{self.nominatim.url}/search",
                params=params,
                headers=headers,
                timeout=self.timeout,
            )

            if r.status_code != 200:
                return None

            data = r.json()

            if not data:
                return None

            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])

            if self._is_generic_location(lat, lon):
                return None

            return lat, lon

        except Exception as e:
            logger.warning(f"[NOMINATIM_STRUCT][ERRO] {e}")
            return None

    # ---------------------------------------------------------
    # GEOCODE INDIVIDUAL
    # ---------------------------------------------------------

    def geocode(self, endereco):
        trace = f"GEO-{int(time.time() * 1000)}"

        if not endereco or not str(endereco).strip():
            with self.stats_lock:
                self.stats["total"] += 1
                self.stats["falha"] += 1
            return None, None, "falha"

        raw = str(endereco).strip()

        endereco_base = normalize_base(raw)
        endereco_geo = normalize_for_geocoding(endereco_base)
        endereco_cache = normalize_for_cache(endereco_base)

        cidade, uf = self._extrair_cidade_uf(endereco_geo)

        with self.stats_lock:
            self.stats["total"] += 1

        # -----------------------------------------------------
        # CACHE GLOBAL
        # -----------------------------------------------------

        cache = self.reader.buscar_cache(endereco_cache)

        if cache:
            lat, lon = cache

            with self.stats_lock:
                self.stats["cache_db"] += 1

            return lat, lon, "cache"

        # -----------------------------------------------------
        # NOMINATIM ESTRUTURADO
        # -----------------------------------------------------

        res = self._buscar_nominatim_estruturado(endereco_geo)

        if res:
            lat, lon = res

            # Se conseguiu extrair cidade/UF, valida capital.
            # Se não conseguiu extrair, aceita o hit.
            if cidade and uf:
                if ponto_dentro_capital(lat, lon, cidade, uf):
                    with self.stats_lock:
                        self.stats["nominatim_struct"] += 1
                    return lat, lon, "nominatim_struct"
            else:
                with self.stats_lock:
                    self.stats["nominatim_struct"] += 1
                return lat, lon, "nominatim_struct"

            logger.warning(
                f"[{trace}][NOMINATIM_STRUCT][REJEITADO_FORA_CAPITAL] "
                f"{cidade}-{uf} lat={lat} lon={lon}"
            )

        # -----------------------------------------------------
        # NOMINATIM LIVRE
        # -----------------------------------------------------

        res = self.nominatim.geocode(endereco_geo)

        if res:
            lat, lon = res

            if not self._is_generic_location(lat, lon):
                with self.stats_lock:
                    self.stats["nominatim_free"] += 1
                return lat, lon, "nominatim"

        # -----------------------------------------------------
        # GOOGLE
        # -----------------------------------------------------

        if self.GOOGLE_ENABLED:
            res = self.google.geocode(endereco_geo)

            if res:
                lat, lon = res

                with self.stats_lock:
                    self.stats["google"] += 1

                return lat, lon, "google"

        with self.stats_lock:
            self.stats["falha"] += 1

        return None, None, "falha"

    # ---------------------------------------------------------
    # GEOCODE BATCH OTIMIZADO
    # ---------------------------------------------------------

    def geocode_batch(self, enderecos):
        """
        Retorna dict:
            {idx_original: (lat, lon, source)}
        """
        resultados = {}

        if not enderecos:
            return resultados

        base = [normalize_base(str(e).strip()) for e in enderecos]
        cache_keys = [normalize_for_cache(e) for e in base]

        cache_map = self.reader.buscar_cache_em_lote(cache_keys)

        misses = []

        for idx, key in enumerate(cache_keys):
            if key in cache_map:
                lat, lon = cache_map[key]
                resultados[idx] = (lat, lon, "cache")
            else:
                misses.append((idx, enderecos[idx]))

        def worker(idx, endereco_raw):
            lat, lon, src = self.geocode(endereco_raw)
            return idx, lat, lon, src

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(worker, idx, endereco_raw): idx
                for idx, endereco_raw in misses
            }

            for f in as_completed(futures):
                try:
                    idx, lat, lon, src = f.result()
                    resultados[idx] = (lat, lon, src)
                except Exception as e:
                    idx = futures[f]
                    logger.error(f"[BATCH][ERRO][idx={idx}] {e}")
                    resultados[idx] = (None, None, "erro")

        return resultados

    # ---------------------------------------------------------
    # STATS
    # ---------------------------------------------------------

    def show_stats(self):
        total = self.stats["total"]

        logger.info("Resumo Geocoding")

        for k, v in self.stats.items():
            if k == "total":
                continue

            pct = (v / total * 100) if total else 0
            logger.info(f"{k}: {v} ({pct:.1f}%)")
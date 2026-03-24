#sales_router/src/geocoding_engine/services/geocoding_service.py

import os
import time
import requests
import threading
import re
import unicodedata

from concurrent.futures import ThreadPoolExecutor, as_completed
from loguru import logger

from geocoding_engine.domain.address_normalizer import (
    normalize_base,
    normalize_for_cache,
    normalize_for_geocoding,
)
from geocoding_engine.domain.geo_validator import GeoValidator
from geocoding_engine.infrastructure.nominatim_client import NominatimClient
from geocoding_engine.infrastructure.google_client import GoogleClient

import redis

_redis = redis.Redis(host="redis", port=6379, decode_responses=True)

def rate_limit(key="geo_rate", limit=40, window=1):

    try:
        current = _redis.incr(key)

        if current == 1:
            _redis.expire(key, window)

        if current > limit:
            time.sleep(window)

    except Exception:
        # fallback: não trava o processo se Redis falhar
        pass


class GeolocationService:
    """
    Motor de geocoding do geocoding_engine.

    Ordem:
      1. Cache global
      2. Nominatim estruturado (street/city/state)
      3. Nominatim estruturado sem número
      4. Nominatim livre
      5. Google
      6. Falha

    Regras:
      - bloqueia coordenadas genéricas
      - valida UF bounds
      - valida polígono IBGE do município
      - não retorna coordenada inválida
    """

    def __init__(self, reader, max_workers=8):
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
    # normalização de texto
    # ---------------------------------------------------------

    @staticmethod
    def _to_ascii_upper(txt):
        if not txt:
            return None
        txt = unicodedata.normalize("NFKD", str(txt))
        txt = "".join(c for c in txt if not unicodedata.combining(c))
        return txt.upper().strip()

    # ---------------------------------------------------------
    # extração robusta de cidade / UF
    # ---------------------------------------------------------

    def _extrair_cidade_uf(self, endereco):
        if not endereco:
            return None, None

        m = re.search(r",\s*([^,]+?)\s*-\s*([A-Za-z]{2})\s*(?:,|$)", endereco)
        if m:
            cidade = m.group(1).strip()
            uf = m.group(2).strip()
            return cidade, uf

        m = re.search(r"([^,]+)\s*-\s*([A-Z]{2})", endereco)
        if m:
            return m.group(1).strip(), m.group(2).strip()

        m = re.search(r"([^,]+),\s*([A-Z]{2})", endereco)
        if m:
            return m.group(1).strip(), m.group(2).strip()

        m = re.search(r"([A-Za-zÀ-ÿ\s]+)\s+([A-Z]{2})$", endereco)
        if m:
            return m.group(1).strip(), m.group(2).strip()

        return None, None

    # ---------------------------------------------------------
    # extrair street / city / state para nominatim estruturado
    # ---------------------------------------------------------

    def _extrair_componentes_endereco(self, endereco):
        """
        Espera algo como:
        'Rua X, 123, Bairro, São Paulo - SP'
        ou
        'Praça da Sé, São Paulo - SP'
        """

        if not endereco:
            return None, None, None

        m = re.search(r",\s*([^,]+?)\s*-\s*([A-Za-z]{2})\s*(?:,|$)", endereco)

        if not m:
            return None, None, None

        city = m.group(1).strip()
        state = m.group(2).strip()

        street_part = endereco[:m.start()].strip().strip(",")

        if not street_part:
            return None, city, state

        street = street_part.split(",")[0].strip()

        if not street:
            return None, city, state

        return street, city, state

    # ---------------------------------------------------------
    # remover número do logradouro
    # ---------------------------------------------------------

    @staticmethod
    def _remover_numero_street(street):
        if not street:
            return None

        street_sem_num = re.sub(r"\b\d+\w?\b", "", street)
        street_sem_num = re.sub(r"\s+", " ", street_sem_num).strip(" ,-")

        return street_sem_num or None

    # ---------------------------------------------------------
    # coordenadas genéricas
    # ---------------------------------------------------------

    @staticmethod
    def _is_generic_location(lat, lon):
        pontos_genericos = [
            (-23.5506507, -46.6333824),  # Sé - SP
            (-22.908333, -43.196388),    # Centro - RJ
            (-15.7801, -47.9292),        # Brasília
            (-19.9167, -43.9345),        # BH
        ]

        for ref_lat, ref_lon in pontos_genericos:
            if abs(lat - ref_lat) < 0.0005 and abs(lon - ref_lon) < 0.0005:
                return True

        return False

    # ---------------------------------------------------------
    # validação geográfica centralizada
    # ---------------------------------------------------------

    def _validar_geo(self, lat, lon, cidade, uf, trace):
        if lat is None or lon is None:
            logger.warning(f"[{trace}][GEO_INVALIDO] lat/lon nulos")
            return False

        if self._is_generic_location(lat, lon):
            logger.warning(
                f"[{trace}][GEO_INVALIDO][GENERICA] lat={lat} lon={lon}"
            )
            return False

        status = GeoValidator.validar_ponto(
            lat,
            lon,
            cidade,
            uf
        )

        if status != "ok":
            logger.warning(
                f"[{trace}][GEO_INVALIDO] cidade={cidade} uf={uf} "
                f"lat={lat} lon={lon} status={status}"
            )
            return False

        return True

    # ---------------------------------------------------------
    # helper nominatim request
    # ---------------------------------------------------------

    def _nominatim_request(self, params, trace, modo):
        headers = {"User-Agent": "SalesRouter-Geocoder"}

        try:
            r = requests.get(
                f"{self.nominatim.url}/search",
                params=params,
                headers=headers,
                timeout=self.timeout,
            )

            if r.status_code != 200:
                logger.warning(
                    f"[{trace}][NOMINATIM][{modo}][HTTP_{r.status_code}] params={params}"
                )
                return None

            data = r.json()

            if not data:
                logger.info(
                    f"[{trace}][NOMINATIM][{modo}][MISS]"
                )
                return None

            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])

            logger.info(
                f"[{trace}][NOMINATIM][{modo}][HIT] lat={lat} lon={lon}"
            )

            return lat, lon

        except Exception as e:
            logger.warning(
                f"[{trace}][NOMINATIM][{modo}][ERRO] {e}"
            )
            return None

    # ---------------------------------------------------------
    # nominatim estruturado
    # ---------------------------------------------------------

    def _buscar_nominatim_estruturado(self, endereco, trace):
        street, city, state = self._extrair_componentes_endereco(endereco)

        if not city or not state:
            logger.info(
                f"[{trace}][NOMINATIM_STRUCT][SKIP] city/state ausentes"
            )
            return None

        if not street:
            logger.info(
                f"[{trace}][NOMINATIM_STRUCT][SKIP] street ausente"
            )
            return None

        # tentativa 1: com número
        params = {
            "street": street,
            "city": city,
            "state": state,
            "country": "Brazil",
            "format": "json",
            "limit": 1,
            "addressdetails": 1,
        }

        res = self._nominatim_request(params, trace, "STRUCT_COM_NUMERO")
        if res:
            return res

        # tentativa 2: sem número
        street_sem_num = self._remover_numero_street(street)

        if street_sem_num and street_sem_num != street:
            params_sem_num = {
                "street": street_sem_num,
                "city": city,
                "state": state,
                "country": "Brazil",
                "format": "json",
                "limit": 1,
                "addressdetails": 1,
            }

            res = self._nominatim_request(
                params_sem_num,
                trace,
                "STRUCT_SEM_NUMERO"
            )
            if res:
                return res

        return None

    # ---------------------------------------------------------
    # nominatim livre
    # ---------------------------------------------------------

    def _buscar_nominatim_livre(self, endereco, trace):
        try:
            res = self.nominatim.geocode(endereco)

            if not res:
                logger.info(f"[{trace}][NOMINATIM_FREE][MISS]")
                return None

            lat, lon = res

            logger.info(
                f"[{trace}][NOMINATIM_FREE][HIT] lat={lat} lon={lon}"
            )

            return lat, lon

        except Exception as e:
            logger.warning(f"[{trace}][NOMINATIM_FREE][ERRO] {e}")
            return None

    # ---------------------------------------------------------
    # google fallback
    # ---------------------------------------------------------

    def _buscar_google(self, endereco, trace):
        if not self.GOOGLE_ENABLED:
            logger.info(f"[{trace}][GOOGLE][SKIP] desabilitado")
            return None

        try:
            res = self.google.geocode(endereco)

            if not res:
                logger.info(f"[{trace}][GOOGLE][MISS]")
                return None

            lat, lon = res

            logger.info(f"[{trace}][GOOGLE][HIT] lat={lat} lon={lon}")

            return lat, lon

        except Exception as e:
            logger.warning(f"[{trace}][GOOGLE][ERRO] {e}")
            return None

    # ---------------------------------------------------------
    # geocode individual
    # ---------------------------------------------------------

    def geocode(self, endereco):
        trace = f"GEO-{int(time.time() * 1000)}"

        if not endereco or not str(endereco).strip():
            with self.stats_lock:
                self.stats["total"] += 1
                self.stats["falha"] += 1

            logger.warning(f"[{trace}][FALHA] endereco vazio")
            return None, None, "falha"

        raw = str(endereco).strip()

        endereco_base = normalize_base(raw)
        endereco_geo = normalize_for_geocoding(endereco_base)
        endereco_cache = normalize_for_cache(endereco_base)

        cidade, uf = self._extrair_cidade_uf(endereco_geo)

        logger.info(
            f"[{trace}][INICIO] raw='{raw}' normalizado='{endereco_geo}' "
            f"cidade='{cidade}' uf='{uf}' cache_key='{endereco_cache}'"
        )

        with self.stats_lock:
            self.stats["total"] += 1

        # -----------------------------------------------------
        # 1. CACHE GLOBAL
        # -----------------------------------------------------

        try:
            cache = self.reader.buscar_cache(endereco_cache)

            if cache:
                lat, lon = cache

                logger.info(
                    f"[{trace}][CACHE][HIT] lat={lat} lon={lon}"
                )

                if self._validar_geo(lat, lon, cidade, uf, trace):
                    with self.stats_lock:
                        self.stats["cache_db"] += 1
                    return lat, lon, "cache"

                logger.warning(
                    f"[{trace}][CACHE][REJEITADO]"
                )

        except Exception as e:
            logger.warning(f"[{trace}][CACHE][ERRO] {e}")

        # -----------------------------------------------------
        # 2. NOMINATIM ESTRUTURADO
        # -----------------------------------------------------

        res = self._buscar_nominatim_estruturado(endereco_geo, trace)

        if res:
            lat, lon = res

            if self._validar_geo(lat, lon, cidade, uf, trace):
                with self.stats_lock:
                    self.stats["nominatim_struct"] += 1
                return lat, lon, "nominatim_struct"

            logger.warning(
                f"[{trace}][NOMINATIM_STRUCT][REJEITADO]"
            )

        # -----------------------------------------------------
        # 3. NOMINATIM LIVRE
        # -----------------------------------------------------

        res = self._buscar_nominatim_livre(endereco_geo, trace)

        if res:
            lat, lon = res

            if self._validar_geo(lat, lon, cidade, uf, trace):
                with self.stats_lock:
                    self.stats["nominatim_free"] += 1
                return lat, lon, "nominatim"

            logger.warning(
                f"[{trace}][NOMINATIM_FREE][REJEITADO]"
            )

        # -----------------------------------------------------
        # 4. GOOGLE
        # -----------------------------------------------------

        res = self._buscar_google(endereco_geo, trace)

        if res:
            lat, lon = res

            if self._validar_geo(lat, lon, cidade, uf, trace):
                with self.stats_lock:
                    self.stats["google"] += 1
                return lat, lon, "google"

            logger.warning(
                f"[{trace}][GOOGLE][REJEITADO]"
            )

        # -----------------------------------------------------
        # FALHA FINAL
        # -----------------------------------------------------

        with self.stats_lock:
            self.stats["falha"] += 1

        logger.error(
            f"[{trace}][FALHA_FINAL] raw='{raw}' normalizado='{endereco_geo}' "
            f"cidade='{cidade}' uf='{uf}'"
        )

        return None, None, "falha"

    # ---------------------------------------------------------
    # batch otimizado
    # ---------------------------------------------------------

    def geocode_batch(self, enderecos):
        """
        Retorna:
            {idx_original: (lat, lon, source)}
        """
        resultados = {}

        if not enderecos:
            return resultados

        base = [normalize_base(str(e).strip()) for e in enderecos]
        cache_keys = [normalize_for_cache(e) for e in base]

        try:
            cache_map = self.reader.buscar_cache_em_lote(cache_keys)
        except Exception as e:
            logger.warning(f"[BATCH][CACHE_LOTE][ERRO] {e}")
            cache_map = {}

        misses = []

        for idx, key in enumerate(cache_keys):
            if key in cache_map:
                lat, lon = cache_map[key]

                endereco_base = base[idx]
                endereco_geo = normalize_for_geocoding(endereco_base)
                cidade, uf = self._extrair_cidade_uf(endereco_geo)
                trace = f"BATCH-{idx}-{int(time.time() * 1000)}"

                if self._validar_geo(lat, lon, cidade, uf, trace):
                    resultados[idx] = (lat, lon, "cache")
                else:
                    misses.append((idx, enderecos[idx]))
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

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    idx_ret, lat, lon, src = future.result()
                    resultados[idx_ret] = (lat, lon, src)
                except Exception as e:
                    logger.error(f"[BATCH][ERRO][idx={idx}] {e}")
                    resultados[idx] = (None, None, "erro")

        return resultados

    # ---------------------------------------------------------
    # resumo stats
    # ---------------------------------------------------------

    def show_stats(self):
        total = self.stats["total"]

        logger.info("Resumo Geocoding")

        for k, v in self.stats.items():
            if k == "total":
                continue

            pct = (v / total * 100) if total else 0
            logger.info(f"{k}: {v} ({pct:.1f}%)")
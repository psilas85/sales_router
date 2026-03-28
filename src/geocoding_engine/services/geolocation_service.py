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
from geocoding_engine.domain.capital_polygon_validator import ponto_dentro_capital
from geocoding_engine.domain.municipio_polygon_validator import ponto_dentro_municipio

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
      1. Cache
        2. Nominatim estruturado
        3. Nominatim sem número
        4. Google
        5. Falha

    Regras:
      - bloqueia coordenadas genéricas
      - valida UF bounds
      - valida polígono IBGE do município
      - não retorna coordenada inválida
    """

    # =========================================================
    # AJUSTE NO INIT
    # =========================================================

    def __init__(self, reader, max_workers=8):  # 🔥 reduzir aqui
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
    # validação numérica (CRÍTICO)
    # ---------------------------------------------------------

    def _is_valid_number(self, v):
        import math
        return (
            isinstance(v, (int, float)) and
            not math.isnan(v) and
            not math.isinf(v)
        )

    # ---------------------------------------------------------
    # validação geográfica centralizada
    # ---------------------------------------------------------

    def _validar_geo(self, lat, lon, cidade, uf, trace):

        if not self._is_valid_number(lat) or not self._is_valid_number(lon):
            return False

        if self._is_generic_location(lat, lon):
            return False

        status = GeoValidator.validar_ponto(lat, lon, cidade, uf)

        if status != "ok":
            logger.debug(f"[VALIDACAO_UF][REJEITADO] lat={lat} lon={lon} uf={uf}")
            return False

        return True

    # ---------------------------------------------------------
    # helper nominatim request
    # ---------------------------------------------------------

    def _nominatim_request(self, params, trace, modo):

        headers = {"User-Agent": "SalesRouter-Geocoder"}

        try:
            rate_limit()

            r = requests.get(
                f"{self.nominatim.url}/search",
                params=params,
                headers=headers,
                timeout=self.timeout,
            )

            if r.status_code != 200:
                logger.warning(f"[NOMINATIM][{modo}][HTTP_{r.status_code}]")
                return None

            data = r.json()

            if not data:
                return None

            item = data[0]

            import math

            try:
                lat = float(item["lat"])
                lon = float(item["lon"])
            except:
                return None

            # 🔥 VALIDAÇÃO NA RAIZ
            if (
                math.isnan(lat) or
                math.isnan(lon) or
                math.isinf(lat) or
                math.isinf(lon)
            ):
                logger.error(
                    f"[ERRO_COORD_INVALIDA][NOMINATIM_RAW] "
                    f"params={params} lat={lat} lon={lon}"
                )
                return None

            address = item.get("address", {})

            cidade_res = (
                address.get("city")
                or address.get("town")
                or address.get("village")
                or address.get("municipality")
            )

            # 🔥 AQUI
            if not cidade_res:
                logger.warning(f"[NOMINATIM][SEM_CIDADE] params={params}")

            state_res = address.get("state")

            tipo = item.get("type")
            addresstype = item.get("addresstype")

            logger.debug(
                f"[NOMINATIM][{modo}] HIT lat={lat} lon={lon} cidade='{cidade_res}' tipo='{addresstype}'"
            )

            return lat, lon, cidade_res, state_res, tipo, addresstype

        except Exception as e:
            logger.warning(f"[NOMINATIM][{modo}][ERRO] {e}")
            return None

    # ---------------------------------------------------------
    # nominatim estruturado (CORRIGIDO COMPLETO)
    # ---------------------------------------------------------

    def _buscar_nominatim_estruturado(self, endereco, trace, cidade=None, uf=None, cep=None):

        street, city, state = self._extrair_componentes_endereco(endereco)

        # -----------------------------------------------------
        # fallback: se não extrair, tenta query livre
        # -----------------------------------------------------
        if not city or not state:

            logger.info(f"[{trace}][NOMINATIM][FALLBACK_QUERY] {endereco}")

            params = {
                "q": endereco,
                "country": "Brazil",
                "format": "json",
                "limit": 1,
                "addressdetails": 1,
            }

            res = self._nominatim_request(params, trace, "FREE_QUERY")

            if res:
                lat, lon, *_ = res
                return lat, lon

            return None

        # =====================================================
        # 1. COM NÚMERO
        # =====================================================
        if street:

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
                lat, lon, cidade_res, state_res, *_ = res

                if uf and state_res:
                    if uf.lower() not in state_res.lower():
                        return None

                return lat, lon

        # =====================================================
        # 2. SEM NÚMERO
        # =====================================================
        street_sem_num = self._remover_numero_street(street)

        if street_sem_num and street_sem_num != street:

            params = {
                "street": street_sem_num,
                "city": city,
                "state": state,
                "country": "Brazil",
                "format": "json",
                "limit": 1,
                "addressdetails": 1,
            }

            res = self._nominatim_request(params, trace, "STRUCT_SEM_NUMERO")

            if res:
                lat, lon, cidade_res, state_res, *_ = res

                if uf and state_res:
                    if uf.lower() not in state_res.lower():
                        return None

                return lat, lon

        # =====================================================
        # 3. CEP
        # =====================================================
        if cep:

            params = {
                "postalcode": cep,
                "country": "Brazil",
                "format": "json",
                "limit": 1,
                "addressdetails": 1,
            }

            res = self._nominatim_request(params, trace, "CEP")

            if res:
                lat, lon, *_ = res
                return lat, lon

        # =====================================================
        # 4. QUERY LIVRE FINAL
        # =====================================================
        params = {
            "q": endereco,
            "country": "Brazil",
            "format": "json",
            "limit": 1,
            "addressdetails": 1,
        }

        res = self._nominatim_request(params, trace, "FREE_QUERY_FINAL")

        if res:
            lat, lon, *_ = res
            return lat, lon

        return None
                
    # ---------------------------------------------------------
    # google fallback (CORRIGIDO COMPLETO)
    # ---------------------------------------------------------

    def _buscar_google(self, endereco, trace, cidade=None, uf=None, cep=None):

        if not self.GOOGLE_ENABLED:
            return None

        try:

            queries = []

            # 1. endereço completo
            if endereco:
                queries.append(endereco)

            # 2. sem número
            street = endereco.split(",")[0] if endereco else None
            if street:
                street_sem_num = re.sub(r"\b\d+\w?\b", "", street)
                street_sem_num = re.sub(r"\s+", " ", street_sem_num).strip(" ,-")
                if street_sem_num and street_sem_num != street:
                    queries.append(street_sem_num)

            # 3. cidade + UF
            if cidade and uf:
                queries.append(f"{cidade}, {uf}")

            # 4. CEP
            if cep:
                queries.append(cep)

            for q in queries:

                if not q:
                    continue

                logger.info(f"[{trace}][GOOGLE][CALL] query='{q}'")

                res = self.google.geocode(q)

                if not res:
                    continue

                # 🔥 suporta tuple e dict
                if isinstance(res, tuple):
                    lat, lon = res
                else:
                    import math

                    if isinstance(res, tuple):
                        lat, lon = res
                    else:
                        lat = res.get("lat")
                        lon = res.get("lon")

                    if lat is None or lon is None:
                        continue

                    # 🔥 VALIDAÇÃO NA RAIZ
                    if (
                        not isinstance(lat, (int, float)) or
                        not isinstance(lon, (int, float)) or
                        math.isnan(lat) or
                        math.isnan(lon) or
                        math.isinf(lat) or
                        math.isinf(lon)
                    ):
                        logger.error(
                            f"[ERRO_COORD_INVALIDA][GOOGLE_RAW] "
                            f"query='{q}' lat={lat} lon={lon}"
                        )
                        continue

                if lat is None or lon is None:
                    continue

                logger.info(f"[{trace}][GOOGLE][HIT] lat={lat} lon={lon}")

                return lat, lon

        except Exception as e:
            logger.warning(f"[{trace}][GOOGLE][ERRO] {e}")

        return None
    # ---------------------------------------------------------
    # geocode individual (REFATORADO SEGURO)
    # ---------------------------------------------------------

    def geocode(self, endereco, cidade=None, uf=None, cep=None):

        trace = f"GEO-{int(time.time() * 1000)}"

        # -----------------------------------------------------
        # VALIDAÇÃO INPUT
        # -----------------------------------------------------
        if not endereco or not str(endereco).strip():
            with self.stats_lock:
                self.stats["total"] += 1
                self.stats["falha"] += 1

            return None, None, "falha", False

        raw = str(endereco).strip()

        # 🔥 remove ".0" de números (CRÍTICO)
        raw = re.sub(r"\.0\b", "", raw)

        endereco_base = normalize_base(raw)
        endereco_geo = normalize_for_geocoding(endereco_base)
        endereco_cache = normalize_for_cache(endereco_base, cep=cep)

        cidade_extr, uf_extr = self._extrair_cidade_uf(endereco_geo)

        cidade = cidade or cidade_extr
        uf = uf or uf_extr

        with self.stats_lock:
            self.stats["total"] += 1

        # -----------------------------------------------------
        # 1. CACHE
        # -----------------------------------------------------
        try:
            cache = self.reader.buscar_cache(endereco_cache)

            if cache:
                lat, lon = cache

                if self._validar_geo(lat, lon, cidade, uf, trace):
                    with self.stats_lock:
                        self.stats["cache_db"] += 1

                    return lat, lon, "cache", True

        except Exception:
            pass

        # -----------------------------------------------------
        # 2. NOMINATIM (já inclui fallback interno)
        # -----------------------------------------------------
        res = self._buscar_nominatim_estruturado(
            endereco_geo,
            trace,
            cidade=cidade,
            uf=uf,
            cep=cep
        )

        if res:
            lat, lon = res

            if self._validar_geo(lat, lon, cidade, uf, trace):
                with self.stats_lock:
                    self.stats["nominatim_struct"] += 1

                return lat, lon, "nominatim_struct", True

        # -----------------------------------------------------
        # 3. GOOGLE
        # -----------------------------------------------------
        # 🔥 só chama Google se endereço tiver número
        # 🔥 SEMPRE tenta Google se Nominatim falhar
        res = self._buscar_google(
            endereco_geo,
            trace,
            cidade=cidade,
            uf=uf,
            cep=cep
        )

        if res:
            lat, lon = res

            if self._validar_geo(lat, lon, cidade, uf, trace):
                with self.stats_lock:
                    self.stats["google"] += 1

                return lat, lon, "google", True

        # -----------------------------------------------------
        # FALHA FINAL
        # -----------------------------------------------------
        with self.stats_lock:
            self.stats["falha"] += 1

        logger.error(
            f"[FALHA] geo='{endereco_geo}' cidade='{cidade}' uf='{uf}'"
        )

        return None, None, "falha", False

    # ---------------------------------------------------------
    # batch otimizado
    # ---------------------------------------------------------

    def geocode_batch(self, enderecos):
        """
        Retorna:
            {idx_original: (lat, lon, source, is_valid)}
        """
        resultados = {}

        if not enderecos:
            return resultados

        base = [normalize_base(str(e).strip()) for e in enderecos]
        cache_keys = [normalize_for_cache(e) for e in base]

        # ---------------------------------------------------------
        # CACHE EM LOTE
        # ---------------------------------------------------------
        try:
            cache_map = self.reader.buscar_cache_em_lote(cache_keys)
        except Exception as e:
            logger.warning(f"[BATCH][CACHE_LOTE][ERRO] {e}")
            cache_map = {}

        misses = []

        # ---------------------------------------------------------
        # CACHE HIT
        # ---------------------------------------------------------
        for idx, key in enumerate(cache_keys):

            if key in cache_map:
                lat, lon = cache_map[key]

                endereco_base = base[idx]
                endereco_geo = normalize_for_geocoding(endereco_base)
                cidade, uf = self._extrair_cidade_uf(endereco_geo)
                trace = f"BATCH-{idx}-{int(time.time() * 1000)}"

                if self._validar_geo(lat, lon, cidade, uf, trace):
                    resultados[idx] = (lat, lon, "cache", True)
                else:
                    resultados[idx] = (None, None, "falha", False)

            else:
                misses.append((idx, enderecos[idx]))

        # ---------------------------------------------------------
        # WORKER
        # ---------------------------------------------------------
        def worker(idx, endereco_raw):
            lat, lon, src, is_valid = self.geocode(endereco_raw)
            return idx, lat, lon, src, is_valid

        # ---------------------------------------------------------
        # PROCESSAMENTO PARALELO
        # ---------------------------------------------------------
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            futures = {
                executor.submit(worker, idx, endereco_raw): idx
                for idx, endereco_raw in misses
            }

            for future in as_completed(futures):
                idx = futures[future]

                try:
                    idx_ret, lat, lon, src, is_valid = future.result()
                    resultados[idx_ret] = (lat, lon, src, is_valid)

                except Exception as e:
                    logger.error(f"[BATCH][ERRO][idx={idx}] {e}")
                    resultados[idx] = (None, None, "erro", False)

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

    def geocode_batch_enriched(self, items):

        resultados = []

        def worker(item):
            try:
                lat, lon, source, valid = self.geocode(
                    item["address"],
                    cidade=item.get("cidade"),
                    uf=item.get("uf"),
                    cep=item.get("cep")
                )

                return {
                    "id": item["id"],
                    "lat": lat,
                    "lon": lon,
                    "source": source,
                    "valid": valid,
                    "cache_key": normalize_for_cache(
                        item["address"],
                        cep=item.get("cep")
                    ),
                    "cidade": item.get("cidade"),
                    "uf": item.get("uf"),
                    "endereco": item.get("address")  # 🔥 AQUI
                }

            except Exception as e:
                logger.error(f"[BATCH][ERRO] id={item.get('id')} erro={e}")

                return {
                    "id": item["id"],
                    "lat": None,
                    "lon": None,
                    "source": "erro",
                    "valid": False,
                    "cache_key": normalize_for_cache(
                        item["address"],
                        cep=item.get("cep")
                    ),
                    "cidade": item.get("cidade"),
                    "uf": item.get("uf"),
                    "endereco": item.get("address")  # 🔥 AQUI TAMBÉM
                }

        # 🔥 PARALELISMO CONTROLADO
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            futures = [executor.submit(worker, item) for item in items]

            for future in as_completed(futures):
                resultados.append(future.result())

        return resultados
#sales_router/src/geocoding_engine/services/geocoding_service.py

import os
import time
import requests
import threading
import re
import unicodedata

from geocoding_engine.domain.municipio_polygon_validator import carregar_municipios_gdf

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


import redis

_redis = redis.Redis(host="redis", port=6379, decode_responses=True)

def rate_limit(key="geo_rate", limit=10, window=1):

    try:
        current = _redis.incr(key)

        if current == 1:
            _redis.expire(key, window)

        if current > limit:
            time.sleep(window)

    except Exception:
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

    def __init__(self, reader, writer=None, max_workers=8):
        self.reader = reader
        self.writer = writer  # 🔥 novo

        self.nominatim = NominatimClient()
        self.google = GoogleClient()

        # 🔥 FIX CRÍTICO
        self.nominatim_url = os.getenv("NOMINATIM_LOCAL_URL")

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
        self.gdf_municipios = carregar_municipios_gdf()

    def _normalize_street(self, street: str):

        if not street:
            return None

        street = street.upper().strip()

        street = street.replace("R ", "RUA ")
        street = street.replace("AV ", "AVENIDA ")
        street = street.replace("ROD ", "RODOVIA ")

        street = re.sub(r"\s+", " ", street)

        return street

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

    def _tem_numero(self, endereco):
        return bool(re.search(r"\b\d{1,5}\b", endereco or ""))
    
    def _montar_query_google(self, endereco, cidade=None, uf=None, cep=None):

        if not endereco:
            return None

        # 🔥 REMOVE duplicação de cidade/UF
        endereco_limpo = endereco

        if cep:
            return f"{endereco_limpo}, {cep}, BRASIL"

        return f"{endereco_limpo}, BRASIL"
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

        if not endereco:
            return None, None, None

        endereco = endereco.strip()

        # =====================================================
        # PADRÕES DE CIDADE/UF (cobre 90% dos casos)
        # =====================================================
        patterns = [
            r",\s*([^,]+?)\s*-\s*([A-Za-z]{2})",   # , cidade - UF
            r",\s*([^,]+?)\s*/\s*([A-Za-z]{2})",   # , cidade/UF
            r"\b([^,]+?)\s*-\s*([A-Za-z]{2})$",    # final
            r"\b([^,]+?)\s*/\s*([A-Za-z]{2})$",    # final /
            r",\s*([^,]+?)\s*,\s*([A-Za-z]{2})",   # , cidade, UF
        ]

        city = None
        state = None
        match = None

        for p in patterns:
            m = re.search(p, endereco)
            if m:
                match = m
                city = m.group(1).strip().upper()
                state = m.group(2).strip().upper()
                break

        if not match:
            return None, None, None

        # =====================================================
        # STREET (ANTES DA CIDADE)
        # =====================================================
        street_part = endereco[:match.start()].strip(", ").strip()

        if not street_part:
            return None, city, state

        # 🔥 REMOVE BAIRRO (muito importante)
        street = street_part.split(",")[0].strip()

        # 🔥 NORMALIZA
        street = self._normalize_street(street)

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

    def _nominatim_request(self, params, trace, tag):

        # 🔥 valida URL antes de tudo
        if not getattr(self, "nominatim_url", None):
            logger.error(f"[NOMINATIM][{tag}][ERRO] URL não configurada")
            return None

        rate_limit()
        try:
            r = requests.get(
                f"{self.nominatim_url}/search",
                params=params,
                headers={"User-Agent": "SalesRouter-Geocoder"},
                timeout=5
            )

            # -----------------------------------------------------
            # HTTP inválido
            # -----------------------------------------------------
            if r.status_code != 200:
                logger.warning(f"[NOMINATIM][{tag}][HTTP_{r.status_code}] params={params}")
                return None

            # -----------------------------------------------------
            # JSON inválido
            # -----------------------------------------------------
            try:
                data = r.json()
            except Exception:
                logger.warning(f"[NOMINATIM][{tag}][ERRO_JSON] resposta inválida")
                return None

            # -----------------------------------------------------
            # MISS
            # -----------------------------------------------------
            if not data:
                logger.info(f"[NOMINATIM][{tag}][MISS] params={params}")
                return None

            item = data[0]

            # -----------------------------------------------------
            # EXTRAÇÃO SEGURA
            # -----------------------------------------------------
            try:
                lat = float(item.get("lat"))
                lon = float(item.get("lon"))
            except Exception:
                logger.warning(f"[NOMINATIM][{tag}][ERRO_COORD] item={item}")
                return None

            tipo = item.get("type")
            addresstype = item.get("addresstype")

            # -----------------------------------------------------
            # LOG PADRONIZADO
            # -----------------------------------------------------
            logger.info(
                f"[NOMINATIM][{tag}][HIT] lat={lat} lon={lon} "
                f"type={tipo} addresstype={addresstype}"
            )

            return lat, lon, tipo, addresstype

        except requests.exceptions.Timeout:
            logger.warning(f"[NOMINATIM][{tag}][TIMEOUT]")
            return None

        except requests.exceptions.ConnectionError:
            logger.warning(f"[NOMINATIM][{tag}][CONNECTION_ERROR]")
            return None

        except Exception as e:
            logger.warning(f"[NOMINATIM][{tag}][ERRO] {e}")
            return None

    def _formatar_endereco_nominatim(self, endereco, cidade=None, uf=None):

        if not endereco:
            return None, None, None, None

        endereco = str(endereco).upper().strip()

        # remove múltiplos espaços
        endereco = re.sub(r"\s+", " ", endereco)

        # remove .0
        endereco = re.sub(r"\.0\b", "", endereco)

        # -----------------------------------------------------
        # SEPARA PARTES
        # -----------------------------------------------------
        partes = [p.strip() for p in endereco.split(",") if p.strip()]

        street = None

        if partes:
            street = partes[0]

        # -----------------------------------------------------
        # REMOVE BAIRRO (fica só rua + número)
        # -----------------------------------------------------
        if street:
            street = re.split(r" - |,", street)[0].strip()

        # -----------------------------------------------------
        # NORMALIZA PREFIXOS
        # -----------------------------------------------------
        if street:
            street = street.replace("R ", "RUA ")
            street = street.replace("AV ", "AVENIDA ")
            street = street.replace("ROD ", "RODOVIA ")

        # -----------------------------------------------------
        # GARANTE NÚMERO
        # -----------------------------------------------------
        if street and not re.search(r"\b\d{1,5}\b", street):
            street = None  # evita erro no nominatim

        # -----------------------------------------------------
        # NORMALIZA CIDADE / UF
        # -----------------------------------------------------
        if cidade:
            cidade = str(cidade).upper().strip()

        if uf:
            uf = str(uf).upper().strip()

        # -----------------------------------------------------
        # MONTA STRING FINAL (fallback)
        # -----------------------------------------------------
        endereco_formatado = None

        if street and cidade and uf:
            endereco_formatado = f"{street}, {cidade}, {uf}, BRAZIL"

        return street, cidade, uf, endereco_formatado

    # ---------------------------------------------------------
    # nominatim estruturado (CORRIGIDO COMPLETO)
    # ---------------------------------------------------------

    def _buscar_nominatim_estruturado(self, endereco, trace, cidade=None, uf=None, cep=None):

        street, cidade, uf, endereco_formatado = self._formatar_endereco_nominatim(
            endereco, cidade, uf
        )

        if not street or not cidade or not uf:
            logger.warning(f"[NOMINATIM][SKIP] dados insuficientes")
            return None

        logger.info(f"[NOMINATIM][TRY][STRUCT] {street} | {cidade} | {uf}")

        query = f"{street}, {cidade}, {uf}, BRAZIL"

        try:
            res = self.nominatim.geocode(query)

            if not res:
                logger.info(f"[NOMINATIM][MISS] {query}")
                return None

            lat, lon, tipo, addresstype = res

            # -------------------------------------------------
            # 🔥 NOVA REGRA (SIMPLES E CORRETA)
            # -------------------------------------------------
            INVALID_TYPES = [
                "city",
                "town",
                "village",
                "hamlet",
                "state",
                "region",
                "county",
                "postcode",
                "country"
            ]

            if addresstype in INVALID_TYPES:
                logger.warning(
                    f"[NOMINATIM][REJEITADO] tipo={addresstype} query='{query}'"
                )
                return None

            logger.info(
                f"[NOMINATIM][HIT] lat={lat} lon={lon} tipo={tipo} addresstype={addresstype}"
            )

            return lat, lon

        except Exception as e:
            logger.warning(f"[NOMINATIM][ERRO] query='{query}' erro={e}")

        return None
    # ---------------------------------------------------------
    # google fallback (CORRIGIDO COMPLETO)
    # ---------------------------------------------------------

    def _buscar_google(self, endereco, trace, cidade=None, uf=None, cep=None):

        if not self.GOOGLE_ENABLED:
            return None

        if self.stats["google"] > 1000:
            logger.warning("[GOOGLE][BLOCKED] limite atingido")
            return None

        try:

            query = self._montar_query_google(
                endereco,
                cidade=cidade,
                uf=uf,
                cep=cep
            )

            logger.info(f"[{trace}][GOOGLE][CALL] query='{query}'")

            res = self.google.geocode(query)
            if not res:
                return None

            lat = res.get("lat") if isinstance(res, dict) else res[0]
            lon = res.get("lon") if isinstance(res, dict) else res[1]

            if lat is None or lon is None:
                return None

            # 🔥 filtro leve de cidade
            # 🔥 NÃO BLOQUEAR GOOGLE POR TEXTO
            if cidade:
                texto = str(res)
                if cidade.upper() not in texto.upper():
                    logger.warning(
                        f"[GOOGLE][CIDADE_MISMATCH] cidade={cidade} resposta={texto}"
                    )

            # ✅ AGORA CORRETO (fora do if)
            return lat, lon

        except Exception as e:
            logger.warning(f"[{trace}][GOOGLE][ERRO] {e}")

        return None
    # ---------------------------------------------------------
    # geocode individual (REFATORADO SEGURO)
    # ---------------------------------------------------------

    def geocode(self, endereco, cidade=None, uf=None, cep=None, cache_key=None):

        trace = f"GEO-{int(time.time() * 1000)}"

        # -----------------------------------------------------
        # VALIDAÇÃO INICIAL
        # -----------------------------------------------------
        if not endereco or not str(endereco).strip():
            with self.stats_lock:
                self.stats["total"] += 1
                self.stats["falha"] += 1
            return None, None, "falha", False

        raw = str(endereco).strip()
        raw = re.sub(r"\.0\b", "", raw)

        endereco_base = raw

        # 🔥 usa original para extrair cidade/UF
        cidade_extr, uf_extr = self._extrair_cidade_uf(endereco_base)

        cidade = cidade or cidade_extr
        uf = uf or uf_extr

        with self.stats_lock:
            self.stats["total"] += 1

        if not cidade or not uf:
            with self.stats_lock:
                self.stats["falha"] += 1
            return None, None, "falha", False

        # -----------------------------------------------------
        # NORMALIZA PARA GEOCODING
        # -----------------------------------------------------
        endereco_geo = normalize_for_geocoding(endereco_base)

        # -----------------------------------------------------
        # NOMINATIM
        # -----------------------------------------------------
        res = self._buscar_nominatim_estruturado(
            endereco_geo,
            trace,
            cidade=cidade,
            uf=uf
        )

        if res:
            lat, lon = res

            if self._validar_geo(lat, lon, cidade, uf, trace):
                with self.stats_lock:
                    self.stats["nominatim_struct"] += 1
                return lat, lon, "nominatim_struct", True

        # -----------------------------------------------------
        # GOOGLE
        # -----------------------------------------------------
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
        # FALHA
        # -----------------------------------------------------
        with self.stats_lock:
            self.stats["falha"] += 1

        return None, None, "falha", False

    # ---------------------------------------------------------
    # batch otimizado
    # ---------------------------------------------------------

    def geocode_batch(self, items):

        resultados = {}

        if not items:
            return resultados

        def worker(idx, item):
            try:
                lat, lon, src, valid = self.geocode(
                    item["address"],
                    cidade=item.get("cidade"),
                    uf=item.get("uf"),
                    cep=item.get("cep"),
                    cache_key=item.get("cache_key"),
                )
                return idx, lat, lon, src, valid

            except Exception as e:
                logger.error(f"[BATCH][ERRO][idx={idx}] {e}")
                return idx, None, None, "erro", False

        with ThreadPoolExecutor(max_workers=min(self.max_workers, 5)) as executor:

            futures = {
                executor.submit(worker, idx, item): idx
                for idx, item in enumerate(items)
            }

            for future in as_completed(futures):
                idx = futures[future]
                try:
                    idx_ret, lat, lon, src, valid = future.result()
                    resultados[idx_ret] = (lat, lon, src, valid)
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

        logger.info(
            f"[FINAL_STATS] "
            f"cache={self.stats['cache_db']} "
            f"nominatim={self.stats['nominatim_struct']} "
            f"google={self.stats['google']} "
            f"falha={self.stats['falha']} "
            f"total={self.stats['total']}"
        )

    def geocode_batch_enriched(self, items):

        resultados = []

        def worker(item):
            try:
                lat, lon, source, valid = self.geocode(
                    item["address"],
                    cidade=item.get("cidade"),
                    uf=item.get("uf"),
                    cep=item.get("cep"),
                    cache_key=item.get("cache_key")  # 🔥 ESSENCIAL
                )

                return {
                    "id": item["id"],
                    "lat": lat,
                    "lon": lon,
                    "source": source,
                    "valid": valid,

                    # 🔥 CORREÇÃO CRÍTICA — SEM CEP
                    "cache_key": item["cache_key"],

                    "cidade": item.get("cidade"),
                    "uf": item.get("uf"),
                    "endereco": item.get("address")
                }

            except Exception as e:
                logger.error(f"[BATCH][ERRO] id={item.get('id')} erro={e}")

                return {
                    "id": item["id"],
                    "lat": None,
                    "lon": None,
                    "source": "erro",
                    "valid": False,

                    # 🔥 CORREÇÃO CRÍTICA — SEM CEP
                    "cache_key": item["cache_key"],

                    "cidade": item.get("cidade"),
                    "uf": item.get("uf"),
                    "endereco": item.get("address")
                }

        # 🔥 PARALELISMO CONTROLADO
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:

            futures = [executor.submit(worker, item) for item in items]

            for future in as_completed(futures):
                resultados.append(future.result())

        return resultados
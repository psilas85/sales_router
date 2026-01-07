#sales_router/src/pdv_preprocessing/domain/geolocation_service.py

# ============================================================
# 📦 src/pdv_preprocessing/domain/geolocation_service.py
# ============================================================

import os
import time
import requests
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Tuple, Dict, List
import threading
import re
import unicodedata


from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.domain.address_normalizer import (
    normalize_base,
    normalize_for_cache,
    normalize_for_geocoding,
)
from pdv_preprocessing.domain.capital_polygon_validator import (
    ponto_dentro_capital
)

class GeolocationService:
    """
    Serviço de georreferenciamento — PDV

    Ordem REAL:
      1. Cache banco
      2. Nominatim local (estruturado: street/city/state)
      3. Nominatim local (query livre)
      4. Google
      5. Falha
    """

    def __init__(
        self,
        reader: DatabaseReader,
        writer: DatabaseWriter,
        max_workers: int = 20,
        usar_google: bool = True,
    ):
        self.reader = reader
        self.writer = writer
        self.usar_google = usar_google
        self.GOOGLE_KEY = os.getenv("GMAPS_API_KEY") if usar_google else None

        self.timeout = 5
        self.max_workers = max_workers

        self.stats = {
            "cache_db": 0,
            "nominatim_local": 0,
            "google": 0,
            "falha": 0,
            "total": 0,
        }
        self.stats_lock = threading.Lock()

        self.nominatim_url = os.getenv(
            "NOMINATIM_LOCAL_URL", "http://172.31.45.41:8080"
        )
    
    
    # ============================================================
    # 🧭 NOMINATIM — ESTRUTURADO (street / city / state)
    # ============================================================
    def _buscar_nominatim_estruturado(
        self, endereco_normalizado: str
    ) -> Tuple[Optional[float], Optional[float], Optional[str], Optional[str]]:

        try:
            # --------------------------------------------------------
            # Extrai cidade e UF do endereço normalizado
            # --------------------------------------------------------
            m = re.search(
                r",\s*([^,]+?)\s*-\s*([A-Za-z]{2})\s*(?:,|$)",
                endereco_normalizado,
            )
            if not m:
                return None, None, None, None

            city = m.group(1).strip().upper()
            state = m.group(2).strip().upper()

            # --------------------------------------------------------
            # Street = tudo antes de "cidade - UF"
            # --------------------------------------------------------
            street_part = endereco_normalizado[: m.start()].strip().strip(",")
            if not street_part:
                return None, None, city, state

            # Ex: "Rua X 123, Bairro" → "Rua X 123"
            street_first = street_part.split(",")[0].strip()

            street_with_number = street_first
            street_without_number = re.sub(r"\b\d+\b", "", street_first).strip()

            headers = {"User-Agent": "SalesRouter/LocalGeocoder"}

            # --------------------------------------------------------
            # Request helper
            # --------------------------------------------------------
            def _req(street_value: str):
                params = {
                    "street": street_value,
                    "city": city,
                    "state": state,
                    "country": "Brazil",
                    "format": "json",
                    "limit": 1,
                    "addressdetails": 1,
                }

                logger.debug(f"[NOMINATIM][STRUCT][REQ] {params}")

                r = requests.get(
                    f"{self.nominatim_url}/search",
                    params=params,
                    headers=headers,
                    timeout=self.timeout,
                )

                if r.status_code != 200:
                    return None

                data = r.json()
                return data[0] if data else None

            # --------------------------------------------------------
            # Tentativa com número
            # --------------------------------------------------------
            hit = _req(street_with_number)

            # --------------------------------------------------------
            # Fallback sem número
            # --------------------------------------------------------
            if not hit and street_without_number and street_without_number != street_with_number:
                hit = _req(street_without_number)

            if not hit:
                return None, None, city, state

            lat = float(hit.get("lat"))
            lon = float(hit.get("lon"))

            # --------------------------------------------------------
            # Bloqueia coordenadas genéricas (centro de cidade)
            # --------------------------------------------------------
            if self._is_generic_location(lat, lon):
                logger.warning(
                    f"[NOMINATIM][STRUCT][GENERICA] {city}-{state} lat={lat} lon={lon}"
                )
                return None, None, city, state

            return lat, lon, city, state

        except Exception as e:
            logger.warning(f"[NOMINATIM][STRUCT][ERRO] {e}", exc_info=True)
            return None, None, None, None



    # ============================================================
    # 🧠 Coordenadas genéricas (centro de cidade)
    # ============================================================
    @staticmethod
    def _is_generic_location(lat: float, lon: float) -> bool:
        pontos_genericos = [
            (-23.5506507, -46.6333824),  # SP Sé
            (-22.908333, -43.196388),    # RJ Centro
            (-15.7801, -47.9292),        # Brasília
            (-19.9167, -43.9345),        # BH
        ]

        for ref_lat, ref_lon in pontos_genericos:
            if abs(lat - ref_lat) < 0.0005 and abs(lon - ref_lon) < 0.0005:
                return True

        return False

    # ============================================================
    # 🏛️ Helper: capital?
    # ============================================================

    _CAPITAIS = {
        ("RIO BRANCO","AC"),("MACEIO","AL"),("MANAUS","AM"),("MACAPA","AP"),
        ("SALVADOR","BA"),("FORTALEZA","CE"),("BRASILIA","DF"),("VITORIA","ES"),
        ("GOIANIA","GO"),("SAO LUIS","MA"),("BELO HORIZONTE","MG"),
        ("CAMPO GRANDE","MS"),("CUIABA","MT"),("BELEM","PA"),
        ("JOAO PESSOA","PB"),("RECIFE","PE"),("TERESINA","PI"),
        ("CURITIBA","PR"),("RIO DE JANEIRO","RJ"),("NATAL","RN"),
        ("PORTO VELHO","RO"),("BOA VISTA","RR"),("PORTO ALEGRE","RS"),
        ("FLORIANOPOLIS","SC"),("ARACAJU","SE"),("SAO PAULO","SP"),
        ("PALMAS","TO"),
    }

    @staticmethod
    def _to_ascii_upper(txt: str | None) -> str | None:
        if not txt:
            return None
        txt = unicodedata.normalize("NFKD", txt)
        txt = "".join(c for c in txt if not unicodedata.combining(c))
        return txt.upper().strip()

    def is_capital(self, city: str | None, state: str | None) -> bool:
        city = self._to_ascii_upper(city)
        state = self._to_ascii_upper(state)
        if not city or not state:
            return False
        return (city, state) in self._CAPITAIS



    # ============================================================
    # 🌍 Busca principal (CORRIGIDA DEFINITIVA)
    # ============================================================
    def buscar_coordenadas(self, endereco: Optional[str], cep: Optional[str] = None):
        trace = f"GEO-{int(time.time() * 1000)}"

        if not endereco:
            with self.stats_lock:
                self.stats["falha"] += 1
            return None, None, "parametro_vazio"

        raw = endereco.strip()

        endereco_base = normalize_base(raw)
        endereco_normalizado = normalize_for_geocoding(endereco_base)
        cache_key = normalize_for_cache(endereco_base)

        # ==========================================================
        # 🔑 EXTRAÇÃO DEFINITIVA DA CIDADE / UF (DO INPUT)
        # ==========================================================
        cidade_input = None
        uf_input = None

        m = re.search(
            r",\s*([^,]+?)\s*-\s*([A-Za-z]{2})\s*(?:,|$)",
            endereco_normalizado,
        )
        if m:
            cidade_input = m.group(1).strip().upper()
            uf_input = m.group(2).strip().upper()

        logger.info(
            f"[{trace}][INICIO]\n"
            f"  RAW ..............: {raw}\n"
            f"  NORMALIZADO ......: {endereco_normalizado}\n"
            f"  CIDADE_INPUT .....: {cidade_input}\n"
            f"  UF_INPUT .........: {uf_input}\n"
            f"  CACHE_KEY ........: {cache_key}"
        )

        with self.stats_lock:
            self.stats["total"] += 1

        # ==========================================================
        # 1) CACHE DB
        # ==========================================================
        try:
            cache_db = self.reader.buscar_localizacao(cache_key)
            if cache_db:
                lat, lon = cache_db

                # 🔒 PROTEÇÃO: capital do CSV sempre valida polígono
                if self.is_capital(cidade_input, uf_input):
                    if not ponto_dentro_capital(lat, lon, cidade_input, uf_input):
                        logger.error(
                            f"[{trace}][CACHE_DB][REJEITADO_FORA_CAPITAL] "
                            f"{cidade_input}-{uf_input} lat={lat} lon={lon}"
                        )
                    else:
                        with self.stats_lock:
                            self.stats["cache_db"] += 1
                        return lat, lon, "cache_db"
                else:
                    with self.stats_lock:
                        self.stats["cache_db"] += 1
                    return lat, lon, "cache_db"
        except Exception as e:
            logger.warning(f"[{trace}][CACHE_DB][ERRO] {e}")

        # ==========================================================
        # 2) NOMINATIM ESTRUTURADO
        # ==========================================================
        lat, lon, city_geo, state_geo = None, None, None, None
        try:
            lat, lon, city_geo, state_geo = self._buscar_nominatim_estruturado(
                endereco_normalizado
            )
        except Exception as e:
            logger.warning(f"[{trace}][NOMINATIM][STRUCT][ERRO] {e}")

        if lat is not None and lon is not None:

            # 🔒 REGRA DE OURO:
            # capital é definida PELO CSV, não pelo geocoder
            if self.is_capital(cidade_input, uf_input):
                if not ponto_dentro_capital(lat, lon, cidade_input, uf_input):
                    logger.error(
                        f"[{trace}][NOMINATIM][REJEITADO_FORA_CAPITAL] "
                        f"{cidade_input}-{uf_input} lat={lat} lon={lon}"
                    )
                else:
                    self._salvar_cache_detalhado(
                        cache_key=cache_key,
                        endereco_raw=raw,
                        endereco_normalizado=endereco_normalizado,
                        lat=lat,
                        lon=lon,
                        fonte="nominatim_struct",
                        trace=trace,
                    )
                    with self.stats_lock:
                        self.stats["nominatim_local"] += 1
                    return lat, lon, "nominatim_struct"
            else:
                # interior → não valida polígono (como você definiu)
                self._salvar_cache_detalhado(
                    cache_key=cache_key,
                    endereco_raw=raw,
                    endereco_normalizado=endereco_normalizado,
                    lat=lat,
                    lon=lon,
                    fonte="nominatim_struct",
                    trace=trace,
                )
                with self.stats_lock:
                    self.stats["nominatim_local"] += 1
                return lat, lon, "nominatim_struct"

        logger.warning(f"[{trace}][NOMINATIM][MISS_OR_REJECT]")

        # ==========================================================
        # 3) GOOGLE (fallback)
        # ==========================================================
        if self.usar_google and self.GOOGLE_KEY:
            try:
                from urllib.parse import quote

                url = (
                    "https://maps.googleapis.com/maps/api/geocode/json?"
                    f"address={quote(endereco_normalizado)}&key={self.GOOGLE_KEY}"
                )

                r = requests.get(url, timeout=5)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("status") == "OK":
                        result = data["results"][0]
                        loc = result["geometry"]["location"]
                        lat, lon = loc["lat"], loc["lng"]

                        if self.is_capital(cidade_input, uf_input):
                            if not ponto_dentro_capital(lat, lon, cidade_input, uf_input):
                                logger.error(
                                    f"[{trace}][GOOGLE][REJEITADO_FORA_CAPITAL] "
                                    f"{cidade_input}-{uf_input} lat={lat} lon={lon}"
                                )
                                with self.stats_lock:
                                    self.stats["falha"] += 1
                                return None, None, "fora_capital"

                        self._salvar_cache_detalhado(
                            cache_key=cache_key,
                            endereco_raw=raw,
                            endereco_normalizado=endereco_normalizado,
                            lat=lat,
                            lon=lon,
                            fonte="google",
                            trace=trace,
                        )
                        with self.stats_lock:
                            self.stats["google"] += 1
                        return lat, lon, "google"

            except Exception as e:
                logger.error(f"[{trace}][GOOGLE][ERRO] {e}", exc_info=True)

        # ==========================================================
        # FALHA FINAL
        # ==========================================================
        with self.stats_lock:
            self.stats["falha"] += 1

        logger.error(
            f"[{trace}][FALHA_FINAL]\n"
            f"  RAW={raw}\n"
            f"  NORMALIZADO={endereco_normalizado}\n"
            f"  CIDADE_INPUT={cidade_input}\n"
            f"  UF_INPUT={uf_input}"
        )

        return None, None, "falha"



    # ============================================================
    # 💾 Salvar cache
    # ============================================================
    def _salvar_cache_detalhado(
        self,
        cache_key: str,
        endereco_raw: str,
        endereco_normalizado: str,
        lat: float,
        lon: float,
        fonte: str,
        trace: str,
    ):
        if lat is None or lon is None:
            logger.warning(
                f"[{trace}][CACHE_DB][IGNORADO] lat/lon nulos"
            )
            return

        logger.debug(
            f"[{trace}][CACHE_DB][WRITE] "
            f"raw='{endereco_raw}' | "
            f"normalizado='{endereco_normalizado}' | "
            f"cache_key='{cache_key}' | "
            f"lat={lat} lon={lon} | fonte={fonte}"
        )

        try:
            self.writer.salvar_cache(
                endereco_cache=cache_key,
                lat=lat,
                lon=lon,
                origem=fonte,
            )

        except Exception as e:
            logger.warning(
                f"[{trace}][CACHE_DB][WRITE_ERRO] {e}",
                exc_info=True
            )

    # ============================================================
    # ⚡ Execução em lote — PDV (VERSÃO SEGURA)
    # ============================================================
    def geocodificar_em_lote(
        self,
        entradas: List[str],
        tipo: str = "PDV"
    ) -> Dict[int, Tuple[float, float, str]]:

        from pdv_preprocessing.domain.utils_texto import fix_encoding

        if not entradas:
            return {}

        logger.info(f"[BATCH][INICIO] {len(entradas)} endereços")

        resultados: Dict[int, Tuple[float, float, str]] = {}

        def _worker(idx: int, endereco_raw: str):
            endereco = fix_encoding(endereco_raw.strip())
            logger.debug(f"[BATCH][ENDERECO][{idx}] {endereco}")
            return idx, self.buscar_coordenadas(endereco)

        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futuros = {
                executor.submit(_worker, idx, e): idx
                for idx, e in enumerate(entradas)
            }

            for futuro in as_completed(futuros):
                try:
                    idx, resultado = futuro.result()
                    resultados[idx] = resultado
                except Exception as e:
                    idx = futuros[futuro]
                    logger.error(
                        f"[BATCH][ERRO][idx={idx}] erro={e}",
                        exc_info=True
                    )
                    resultados[idx] = (None, None, "erro")

        logger.info("[BATCH][FIM]")
        return resultados


    # ============================================================
    # 📊 Resumo final de logs
    # ============================================================
    def exibir_resumo_logs(self):
        total = self.stats.get("total", 0)

        logger.info("📊 Resumo Geolocalização:")

        for origem, count in self.stats.items():
            if origem == "total":
                continue
            pct = (count / total * 100) if total else 0
            logger.info(f"   {origem:<18}: {count:>6} ({pct:5.1f}%)")

        sucesso = total - self.stats.get("falha", 0)
        taxa = (sucesso / total * 100) if total else 0
        logger.info(f"✅ Sucesso: {sucesso}/{total} ({taxa:.1f}%)")

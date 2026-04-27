# ============================================================
# 📦 src/pdv_preprocessing/domain/mkp_geolocation_service.py
# ============================================================

import os
import re
import time
import requests
import threading
import unicodedata
from loguru import logger
from typing import Optional, Tuple

from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.domain.address_normalizer import (
    normalize_base,
    normalize_for_cache,
    normalize_for_geocoding,
)
from pdv_preprocessing.domain.capital_polygon_validator import ponto_dentro_capital
from pdv_preprocessing.config.uf_bounds import UF_BOUNDS


class MKPGeolocationService:
    """
    Serviço de geolocalização MKP — espelhado do PDV

    Ordem:
      1) Cache DB
      2) Nominatim local estruturado
      3) Nominatim local query livre
      4) Google (opcional)
      5) Falha

    Regras:
      • UF bounds SEMPRE
      • Polígono SOMENTE se capital
      • Capital definida pelo INPUT
    """

    def __init__(
        self,
        reader: DatabaseReader,
        writer: DatabaseWriter,
        max_workers: int = 4,
        usar_google: bool = True,
    ):
        self.reader = reader
        self.writer = writer
        self.max_workers = max_workers
        self.usar_google = usar_google

        self.GOOGLE_KEY = os.getenv("GMAPS_API_KEY") if usar_google else None
        self.nominatim_url = os.getenv("NOMINATIM_LOCAL_URL")

        if not self.nominatim_url:
            raise RuntimeError("NOMINATIM_LOCAL_URL não configurado")

        self.timeout = 5
        self.headers = {"User-Agent": "SalesRouter/MKP-LocalGeocoder"}

        self.stats = {
            "cache_db": 0,
            "nominatim_local": 0,
            "google": 0,
            "falha": 0,
            "total": 0,
        }
        self.stats_lock = threading.Lock()

    # ============================================================
    # 🧼 Utils
    # ============================================================

    @staticmethod
    def _to_ascii_upper(txt: Optional[str]) -> Optional[str]:
        if not txt:
            return None
        txt = unicodedata.normalize("NFKD", txt)
        txt = "".join(c for c in txt if not unicodedata.combining(c))
        return txt.upper().strip()

    def _dentro_uf_bounds(self, lat: float, lon: float, uf: Optional[str]) -> bool:
        if lat is None or lon is None or not uf:
            return False
        uf = uf.strip().upper()
        bounds = UF_BOUNDS.get(uf)
        if not bounds:
            return False
        return (
            bounds["lat_min"] <= lat <= bounds["lat_max"]
            and bounds["lon_min"] <= lon <= bounds["lon_max"]
        )

    @staticmethod
    def _is_generic_location(lat: float, lon: float) -> bool:
        pontos_genericos = [
            (-23.5506507, -46.6333824),
            (-22.908333, -43.196388),
            (-15.7801, -47.9292),
            (-19.9167, -43.9345),
        ]
        return any(
            abs(lat - rlat) < 0.0005 and abs(lon - rlon) < 0.0005
            for rlat, rlon in pontos_genericos
        )

    # ============================================================
    # 🏛️ Capitais
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

    def is_capital(self, city: Optional[str], state: Optional[str]) -> bool:
        city = self._to_ascii_upper(city)
        state = self._to_ascii_upper(state)
        return (city, state) in self._CAPITAIS if city and state else False

    # ============================================================
    # 🔎 Extrai cidade / UF do endereço
    # ============================================================

    @staticmethod
    def _extrair_cidade_uf(endereco: str) -> Tuple[Optional[str], Optional[str]]:
        m = re.search(r",\s*([^,]+?)\s*-\s*([A-Za-z]{2})\s*(?:,|$)", endereco)
        if not m:
            return None, None
        return m.group(1).strip().upper(), m.group(2).strip().upper()

    # ============================================================
    # 🧭 NOMINATIM estruturado
    # ============================================================

    def _buscar_nominatim_estruturado(
        self, endereco_normalizado: str
    ) -> Tuple[Optional[float], Optional[float]]:

        try:
            city, state = self._extrair_cidade_uf(endereco_normalizado)
            if not city or not state:
                return None, None

            street_part = endereco_normalizado.split(f", {city} - {state}")[0]
            street_first = street_part.split(",")[0].strip()

            streets = [
                street_first,
                re.sub(r"\b\d+\b", "", street_first).strip(),
            ]

            for street in streets:
                if not street:
                    continue

                r = requests.get(
                    f"{self.nominatim_url}/search",
                    params={
                        "street": street,
                        "city": city,
                        "state": state,
                        "country": "Brazil",
                        "format": "json",
                        "limit": 1,
                    },
                    headers=self.headers,
                    timeout=self.timeout,
                )

                if r.status_code != 200:
                    continue

                data = r.json()
                if data:
                    lat = float(data[0]["lat"])
                    lon = float(data[0]["lon"])
                    if not self._is_generic_location(lat, lon):
                        return lat, lon

        except Exception as e:
            logger.warning(f"[MKP][NOMINATIM_STRUCT][ERRO] {e}", exc_info=True)

        return None, None

    # ============================================================
    # 🔍 NOMINATIM query livre
    # ============================================================

    def _buscar_nominatim_query(
        self, endereco_normalizado: str
    ) -> Tuple[Optional[float], Optional[float]]:

        try:
            r = requests.get(
                f"{self.nominatim_url}/search",
                params={"q": endereco_normalizado, "format": "json", "limit": 1},
                headers=self.headers,
                timeout=self.timeout,
            )

            if r.status_code != 200:
                return None, None

            data = r.json()
            if not data:
                return None, None

            lat = float(data[0]["lat"])
            lon = float(data[0]["lon"])

            if self._is_generic_location(lat, lon):
                return None, None

            return lat, lon

        except Exception as e:
            logger.warning(f"[MKP][NOMINATIM_Q][ERRO] {e}", exc_info=True)
            return None, None

    # ============================================================
    # 🌍 Busca principal
    # ============================================================

    def buscar_coordenadas(
        self,
        endereco: Optional[str],
        cep: Optional[str] = None,
    ) -> Tuple[Optional[float], Optional[float], str]:

        if not endereco or not endereco.strip():
            self.stats["falha"] += 1
            return None, None, "parametro_vazio"

        raw = endereco.strip()
        base = normalize_base(raw)
        normalizado = normalize_for_geocoding(base)
        cache_key = normalize_for_cache(base)

        cidade, uf = self._extrair_cidade_uf(normalizado)

        with self.stats_lock:
            self.stats["total"] += 1

        # ========================================================
        # 1) CACHE
        # ========================================================
        cache = self.reader.buscar_localizacao(cache_key)
        if cache:
            lat, lon = cache
            if self._dentro_uf_bounds(lat, lon, uf):
                if self.is_capital(cidade, uf):
                    if ponto_dentro_capital(lat, lon, cidade, uf):
                        self.stats["cache_db"] += 1
                        return lat, lon, "cache_db"
                else:
                    self.stats["cache_db"] += 1
                    return lat, lon, "cache_db"

        # ========================================================
        # 2) NOMINATIM estruturado
        # ========================================================
        lat, lon = self._buscar_nominatim_estruturado(normalizado)
        if lat is not None and lon is not None:
            if self._dentro_uf_bounds(lat, lon, uf):
                if self.is_capital(cidade, uf):
                    if ponto_dentro_capital(lat, lon, cidade, uf):
                        self.writer.salvar_cache(cache_key, lat, lon, "nominatim_struct")
                        self.stats["nominatim_local"] += 1
                        return lat, lon, "nominatim_struct"
                else:
                    self.writer.salvar_cache(cache_key, lat, lon, "nominatim_struct")
                    self.stats["nominatim_local"] += 1
                    return lat, lon, "nominatim_struct"

        # ========================================================
        # 3) NOMINATIM query
        # ========================================================
        lat, lon = self._buscar_nominatim_query(normalizado)
        if lat is not None and lon is not None:
            if self._dentro_uf_bounds(lat, lon, uf):
                if self.is_capital(cidade, uf):
                    if ponto_dentro_capital(lat, lon, cidade, uf):
                        self.writer.salvar_cache(cache_key, lat, lon, "nominatim_q")
                        self.stats["nominatim_local"] += 1
                        return lat, lon, "nominatim_q"
                else:
                    self.writer.salvar_cache(cache_key, lat, lon, "nominatim_q")
                    self.stats["nominatim_local"] += 1
                    return lat, lon, "nominatim_q"

        # ========================================================
        # 4) GOOGLE
        # ========================================================
        if self.usar_google and self.GOOGLE_KEY:
            logger.info("[MKP][GOOGLE][CALLED]")
            try:
                from urllib.parse import quote
                url = (
                    "https://maps.googleapis.com/maps/api/geocode/json?"
                    f"address={quote(normalizado)}&key={self.GOOGLE_KEY}"
                )
                r = requests.get(url, timeout=self.timeout)
                if r.status_code == 200:
                    data = r.json()
                    if data.get("status") == "OK":
                        loc = data["results"][0]["geometry"]["location"]
                        lat, lon = loc["lat"], loc["lng"]

                        if self._dentro_uf_bounds(lat, lon, uf):
                            if self.is_capital(cidade, uf):
                                if ponto_dentro_capital(lat, lon, cidade, uf):
                                    self.writer.salvar_cache(cache_key, lat, lon, "google")
                                    self.stats["google"] += 1
                                    return lat, lon, "google"
                            else:
                                self.writer.salvar_cache(cache_key, lat, lon, "google")
                                self.stats["google"] += 1
                                return lat, lon, "google"
            except Exception as e:
                logger.error(f"[MKP][GOOGLE][ERRO] {e}", exc_info=True)

        # ========================================================
        # FALHA
        # ========================================================
        self.stats["falha"] += 1
        return None, None, "falha"

    # ============================================================
    # 📊 Resumo
    # ============================================================

    def exibir_resumo_logs(self):
        total = self.stats["total"]
        logger.info("📊 Resumo MKP Geolocalização:")
        for k, v in self.stats.items():
            if k != "total":
                pct = (v / total * 100) if total else 0
                logger.info(f"   {k:<18}: {v:>6} ({pct:5.1f}%)")

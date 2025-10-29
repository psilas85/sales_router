# ============================================================
# 📦 src/pdv_preprocessing/domain/geolocation_service.py
# ============================================================

import os
import re
import time
import math
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from geopy.exc import GeocoderTimedOut
from random import uniform
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter


class GeolocationService:
    """
    Serviço de georreferenciamento unificado:
      - Cache em memória e banco
      - Fallback: Cache → Nominatim local → Nominatim público → Google
      - Execução paralela com retries e backoff
      - Compatível com PDV e MKP
    """

    def __init__(self, reader: DatabaseReader, writer: DatabaseWriter, max_workers: int = 20):
        self.reader = reader
        self.writer = writer
        self.GOOGLE_KEY = os.getenv("GMAPS_API_KEY")
        self.NOMINATIM_LOCAL = os.getenv("NOMINATIM_LOCAL_URL", "http://nominatim:8080/reverse")
        self.NOMINATIM_PUBLIC = "https://nominatim.openstreetmap.org/search"
        self.timeout = 10
        self.max_workers = max_workers

        self.cache_mem = {}
        
        self.stats = {
            "cache_mem": 0,
            "cache_db": 0,
            "nominatim_public": 0,
            "nominatim_local": 0,
            "google": 0,
            "falha": 0,
            "total": 0,
        }

    # ============================================================
    # 🧭 Validador de coordenadas genéricas
    # ============================================================
    def _is_generic_location(self, lat: float, lon: float) -> bool:
        if lat is None or lon is None:
            return True
        pontos_genericos = [
            (-23.5506507, -46.6333824),  # São Paulo/SP
            (-22.908333, -43.196388),    # Rio de Janeiro/RJ
            (-15.7801, -47.9292),        # Brasília/DF
            (-19.9167, -43.9345),        # Belo Horizonte/MG
        ]
        for ref_lat, ref_lon in pontos_genericos:
            if abs(lat - ref_lat) < 0.0005 and abs(lon - ref_lon) < 0.0005:
                return True
        return False

    # ============================================================
    # 🌍 Buscar coordenadas (PDV ou MKP)
    # ============================================================
    def buscar_coordenadas(self, endereco: str | None, cep: str | None = None) -> tuple[float, float, str]:
        if not endereco and not cep:
            return None, None, "parametro_vazio"

        query = (cep or endereco).strip().lower()
        self.stats["total"] += 1

        # 1️⃣ Cache em memória
        if query in self.cache_mem:
            self.stats["cache_mem"] += 1
            lat, lon = self.cache_mem[query]
            return lat, lon, "cache_mem"

        # 2️⃣ Cache no banco
        cache_db = self.reader.buscar_localizacao(endereco) if endereco else self.reader.buscar_localizacao_mkp(cep)
        if cache_db:
            lat, lon = cache_db
            self.stats["cache_db"] += 1
            self.cache_mem[query] = (lat, lon)
            return lat, lon, "cache_db"

        # Função auxiliar com retry e backoff
        def _get_with_retry(url, headers=None, max_retries=3):
            for tent in range(max_retries):
                try:
                    r = requests.get(url, headers=headers, timeout=self.timeout)
                    if r.status_code == 200:
                        return r.json()
                except Exception as e:
                    logging.debug(f"⚠️ Falha tentativa {tent+1}/{max_retries} → {e}")
                time.sleep(0.5 * (2 ** tent) + uniform(0, 0.3))
            return None

        # 3️⃣ Nominatim público
        headers = {"User-Agent": "SalesRouter-Geocoder/1.0"}
        url_pub = f"{self.NOMINATIM_PUBLIC}?q={query}+Brasil&countrycodes=br&format=json"
        dados = _get_with_retry(url_pub, headers)
        if isinstance(dados, list) and len(dados) > 0:
            lat, lon = float(dados[0]["lat"]), float(dados[0]["lon"])
            if not self._is_generic_location(lat, lon):
                self.stats["nominatim_public"] += 1
                self.cache_mem[query] = (lat, lon)
                self.writer.salvar_cache(endereco or cep, lat, lon, tipo="mkp" if cep else "pdv")
                return lat, lon, "nominatim_public"

        # 4️⃣ Google Maps fallback
        if self.GOOGLE_KEY:
            from urllib.parse import quote
            url_google = f"https://maps.googleapis.com/maps/api/geocode/json?address={quote(query+', Brasil')}&key={self.GOOGLE_KEY}"
            dados = _get_with_retry(url_google)
            if dados and dados.get("status") == "OK" and dados.get("results"):
                loc = dados["results"][0]["geometry"]["location"]
                lat, lon = loc["lat"], loc["lng"]
                if not self._is_generic_location(lat, lon):
                    self.stats["google"] += 1
                    self.cache_mem[query] = (lat, lon)
                    self.writer.salvar_cache(endereco or cep, lat, lon, tipo="mkp" if cep else "pdv")
                    return lat, lon, "google"

        # ❌ Nenhum resultado
        self.stats["falha"] += 1
        logging.debug(f"💀 Nenhuma coordenada encontrada para {query}")
        return None, None, "falha"

    # ============================================================
    # ⚡ Geocodificação em lote (threads)
    # ============================================================
    def geocodificar_em_lote(self, entradas: list[str], tipo: str = "PDV") -> dict[str, tuple[float, float]]:
        if not entradas:
            return {}

        total = len(entradas)
        max_workers = min(self.max_workers, max(5, total // 50))
        inicio_total = time.time()
        resultados = {}

        logging.info(f"🚀 Geocodificação em lote ({tipo}) iniciada: {total} registros | {max_workers} threads")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {
                executor.submit(
                    self.buscar_coordenadas,
                    e if tipo == "PDV" else None,
                    e if tipo == "MKP" else None,
                ): e for e in entradas
            }

            for i, futuro in enumerate(as_completed(futuros), 1):
                chave = futuros[futuro]
                try:
                    coords = futuro.result()
                    if coords:
                        resultados[chave] = coords
                except Exception as e:
                    logging.warning(f"⚠️ Erro geocodificando {chave}: {e}")

                if i % 1000 == 0 or i == total:
                    logging.info(
                        f"🧩 Progresso: {i}/{total} ({100 * i / total:.1f}%) "
                        f"→ {len(resultados)} resolvidos | {self.stats['falha']} falhas"
                    )

        dur = time.time() - inicio_total
        taxa_ok = (len(resultados) / total * 100) if total else 0
        logging.info(
            f"✅ Concluído: {len(resultados)}/{total} resolvidos ({taxa_ok:.1f}%) em {dur:.1f}s "
            f"→ média {dur/total:.2f}s/reg"
        )
        return resultados

    # ============================================================
    # 📊 Resumo de logs
    # ============================================================
    def exibir_resumo_logs(self):
        logging.info("📊 Resumo de Geolocalização:")
        for origem, count in self.stats.items():
            if origem != "total":
                logging.info(f"   {origem:<18}: {count}")
        logging.info(f"   total               : {self.stats['total']}")


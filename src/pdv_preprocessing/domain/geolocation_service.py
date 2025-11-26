#sales_router/src/pdv_preprocessing/domain/geolocation_service.py

# ============================================================
# 📦 src/pdv_preprocessing/domain/geolocation_service.py
# ============================================================

import os
import time
import requests
from loguru import logger
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import uniform
from typing import Optional, Tuple, Dict, List

from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.utils.endereco_normalizer import normalizar_endereco_completo


class GeolocationService:
    """
    Serviço de georreferenciamento unificado:

      Ordem de fallback:
        1. Cache em memória
        2. Cache no banco
        3. API Brasil (para completar endereço)
        4. Nominatim Local (3 tentativas)
        5. Google Maps (se ativado)
        6. Falha
    """

    def __init__(self, reader: DatabaseReader, writer: DatabaseWriter, max_workers: int = 20, usar_google: bool = False):
        self.reader = reader
        self.writer = writer
        self.usar_google = usar_google

        self.GOOGLE_KEY = os.getenv("GMAPS_API_KEY") if usar_google else None
        self.timeout = 5
        self.max_workers = max_workers

        self.cache_mem: Dict[str, Tuple[float, float]] = {}
        self.cache_api_brasil = {}

        self.stats = {
            "cache_mem": 0,
            "cache_db": 0,
            "nominatim_local": 0,
            "google": 0,
            "falha": 0,
            "total": 0,
        }
        import threading
        self.stats_lock = threading.Lock()


    def _buscar_nominatim_simples(self, endereco_raw: str):
        """Primeira tentativa — envia o endereço exatamente como veio no CSV."""
        host = os.getenv("NOMINATIM_LOCAL_URL", "http://172.31.45.41:8080")
        url = f"{host}/search"

        params = {
            "q": endereco_raw,
            "format": "json",
            "limit": 1,
            "addressdetails": 1,
            "countrycodes": "br"
        }

        try:
            headers = {"User-Agent": "SalesRouter/LocalGeocoder"}
            r = requests.get(url, params=params, headers=headers, timeout=4)

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

        except Exception:
            return None, None



    # ============================================================
    # 🔵 API BRASIL
    # ============================================================
    def _buscar_api_brasil(self, cep: str):
        cep_raw = cep
        try:
            if not cep or len(cep) != 8:
                logger.debug(f"🔵 [API_BRASIL] CEP inválido → '{cep_raw}'")
                return None

            url = f"https://brasilapi.com.br/api/cep/v2/{cep}"
            headers = {"User-Agent": "SalesRouter-Geocoder"}

            logger.debug(f"🔵 [API_BRASIL] REQUEST → {url}")

            r = requests.get(url, headers=headers, timeout=3)

            logger.debug(
                f"🔵 [API_BRASIL] RESPONSE → status={r.status_code} body={r.text[:200]}"
            )

            if r.status_code != 200:
                return None

            data = r.json()

            result = {
                "logradouro": data.get("street", ""),
                "bairro": data.get("neighborhood", ""),
                "cidade": data.get("city", ""),
                "uf": data.get("state", "")
            }

            logger.debug(f"🔵 [API_BRASIL] Dados extraídos → {result}")

            return result

        except Exception as e:
            logger.warning(f"⚠️ [API_BRASIL] Erro: {e}")
            return None

    # ============================================================
    # 🔒 PATCH ANTI-LOOP — BrasilAPI chamada apenas 1x por CEP
    # ============================================================
    def _buscar_api_brasil_com_cache(self, cep: str):
        """Evita loops: BrasilAPI será chamada apenas uma vez por CEP."""
        cep = cep.strip().replace("-", "")

        if not cep or len(cep) != 8:
            return None

        # Se já consultou este CEP antes → devolve cache
        if cep in self.cache_api_brasil:
            return self.cache_api_brasil[cep]

        # Chama a API real APENAS uma vez
        dados = self._buscar_api_brasil(cep)

        # Mesmo None é cacheado (evita looping infinito)
        self.cache_api_brasil[cep] = dados

        return dados



    # ============================================================
    # 🧭 Coordenadas muito genéricas
    # ============================================================
    @staticmethod
    def _is_generic_location(lat: float, lon: float) -> bool:
        if lat is None or lon is None:
            return True

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
    # 🌍 NOMINATIM LOCAL — fallback inteligente completo
    # ============================================================
    def _buscar_nominatim_multi(self, logradouro, numero, bairro, cidade, uf, cep=None):
        host = os.getenv("NOMINATIM_LOCAL_URL", "http://172.31.45.41:8080")
        url = f"{host}/search"

        tentativas = []

        # Rua + número + bairro
        if numero:
            tentativas.append(f"{logradouro} {numero}, {bairro}, {cidade} - {uf}, Brasil")

        # Rua + bairro
        if bairro:
            tentativas.append(f"{logradouro}, {bairro}, {cidade} - {uf}, Brasil")

        # Rua + cidade
        if cidade:
            tentativas.append(f"{logradouro}, {cidade} - {uf}, Brasil")

        # CEP via API Brasil
        if cep and len(cep) == 8:
            dados_api = self._buscar_api_brasil_com_cache(cep)

            if dados_api:
                tentativas.append(
                    f"{dados_api['logradouro']} {numero}, {dados_api['bairro']}, "
                    f"{dados_api['cidade']} - {dados_api['uf']}, Brasil"
                )
                tentativas.append(
                    f"{dados_api['logradouro']}, {dados_api['cidade']} - {dados_api['uf']}, Brasil"
                )

        # Só logradouro
        tentativas.append(f"{logradouro}, Brasil")

        logger.debug(f"🌍 [NOMINATIM_MULTI] {len(tentativas)} tentativas geradas.")

        headers = {"User-Agent": "SalesRouter/LocalGeocoder"}

        for tentativa in tentativas:
            try:
                logger.debug(f"🌍 [NOMINATIM_MULTI] Tentando → '{tentativa}'")

                params = {
                    "q": tentativa,
                    "format": "json",
                    "limit": 1,
                    "addressdetails": 1,
                    "countrycodes": "br"
                }

                r = requests.get(url, params=params, headers=headers, timeout=3)
                logger.debug(f"🌍 [NOMINATIM_MULTI] status={r.status_code} retorno={r.text[:200]}")

                if r.status_code != 200:
                    continue

                data = r.json()
                if not data:
                    continue

                lat = float(data[0]["lat"])
                lon = float(data[0]["lon"])

                if not self._is_generic_location(lat, lon):
                    logger.debug(f"🌍 [NOMINATIM_MULTI] SUCESSO: ({lat}, {lon})")
                    return lat, lon

            except Exception as e:
                logger.warning(f"⚠️ [NOMINATIM_MULTI] Erro tentativa '{tentativa}' → {e}")

        logger.debug("❌ [NOMINATIM_MULTI] Nenhuma tentativa retornou coordenadas válidas")
        return None, None


    # ============================================================
    # 🌍 Busca principal
    # ============================================================
    def buscar_coordenadas(self, endereco: Optional[str], cep: Optional[str] = None):
        import re

        # -----------------------------------------------
        # Endereço vazio
        # -----------------------------------------------
        if not endereco:
            logger.warning("⚠️ [GEO] Endereço vazio — nada a fazer.")
            with self.stats_lock:
                self.stats["falha"] += 1
            return None, None, "parametro_vazio"

        raw = endereco.strip()
        query = normalizar_endereco_completo(raw)

        logger.debug("\n\n===============================================")
        logger.debug(f"🔎 [GEO] INÍCIO → '{raw}'")
        logger.debug("===============================================\n")

        with self.stats_lock:
            self.stats["total"] += 1


        # -----------------------------------------------
        # 1) Cache memória
        # -----------------------------------------------
        if query in self.cache_mem:
            lat, lon = self.cache_mem[query]
            logger.debug(f"🧠 [CACHE_MEM] → ({lat}, {lon})")
            with self.stats_lock:
                self.stats["cache_mem"] += 1

            return lat, lon, "cache_mem"


        # -----------------------------------------------
        # 2) Cache DB
        # -----------------------------------------------
        try:
            cache_db = self.reader.buscar_localizacao(query)
            if cache_db:
                lat, lon = cache_db
                logger.debug(f"🗄️ [CACHE_DB] → ({lat}, {lon})")
                self.cache_mem[query] = (lat, lon)
                with self.stats_lock:
                    self.stats["cache_db"] += 1

                return lat, lon, "cache_db"


        except Exception as e:
            logger.warning(f"⚠️ [CACHE_DB] Erro: {e}")

        # ============================================================
        # 3) API BRASIL — Fallback ANTES do parse
        # ============================================================
        if cep:
            cep_clean = cep.replace("-", "").strip()

            if len(cep_clean) == 8 and cep_clean.isdigit():
                logger.debug(f"🔵 [API_BRASIL] CEP recebido: {cep_clean}")

                dados_api = self._buscar_api_brasil_com_cache(cep_clean)


                if dados_api:
                    logger.debug(f"🔵 [API_BRASIL] OK → {dados_api}")

                    # Extração segura de número (não confundir CEP)
                    num_match = re.search(r"\b(\d{1,5})\b", raw)
                    numero_ok = ""
                    if num_match:
                        n = num_match.group(1)
                        # Se for CEP → ignorar
                        if not (len(n) == 5 and re.search(rf"\b{n}[- ]?\d{{3}}\b", raw)):
                            numero_ok = n

                    # Endereço reconstruído real
                    endereco_api = (
                        f"{dados_api['logradouro']} {numero_ok}, "
                        f"{dados_api['bairro']}, "
                        f"{dados_api['cidade']} - {dados_api['uf']}, Brasil"
                    )

                    logger.debug(f"🔵 [API_BRASIL] Endereço reconstruído: {endereco_api}")

                    # Tenta Nominatim com endereço 100% correto
                    lat, lon = self._buscar_nominatim_multi(
                        dados_api["logradouro"],
                        numero_ok,
                        dados_api["bairro"],
                        dados_api["cidade"],
                        dados_api["uf"],
                        cep=cep_clean
                    )

                    if lat:
                        logger.debug(f"🌍 [API_BRASIL→NOMINATIM] SUCESSO ({lat}, {lon})")

                        with self.stats_lock:
                            self.stats["nominatim_local"] += 1


                        norm = normalizar_endereco_completo(endereco_api)
                        self.cache_mem[norm] = (lat, lon)
                        self.cache_mem[query] = (lat, lon)
                        self.writer.salvar_cache(norm, lat, lon, tipo="pdv")

                        return lat, lon, "api_brasil_nominatim"

                else:
                    logger.debug(f"🔵 [API_BRASIL] Nenhum dado encontrado para {cep_clean}")

        # ============================================================
        # 4) PARSE NORMAL — após tentar API Brasil
        # ============================================================
        logger.debug("🧩 [PARSE] Extraindo logradouro/bairro/cidade/uf...")

        # logradouro original
        m_log = re.search(r"^[^,]+", raw)
        logradouro = m_log.group(0).strip() if m_log else ""

        # número (ignora CEPs)
        numero = ""
        m_num = re.search(r"\b(\d{1,5})\b", raw)
        if m_num:
            n = m_num.group(1)
            # Se for CEP → descartar
            if not (len(n) == 5 and re.search(rf"\b{n}[- ]?\d{{3}}\b", raw)):
                numero = n

        # bairro
        m_bairro = re.search(r",\s*([^,]+),", raw)
        bairro = m_bairro.group(1).strip() if m_bairro else ""

        # cidade/uf
        cidade = ""
        uf = ""
        m_cu = re.search(r",\s*([^,]+?)\s*-\s*([A-Za-z]{2})[,\s]", raw + " ")
        if m_cu:
            cidade = m_cu.group(1).strip()
            uf = m_cu.group(2).upper().strip()

        logger.debug(
            f"🧩 [PARSE] logradouro='{logradouro}', numero='{numero}', "
            f"bairro='{bairro}', cidade='{cidade}', uf='{uf}'"
        )

        # ============================================================
        # 5) NOMINATIM MULTI
        # ============================================================
        lat, lon = self._buscar_nominatim_multi(logradouro, numero, bairro, cidade, uf)

        if lat:
            logger.debug(f"🌍 [NOMINATIM_SIMPLE] SUCESSO ({lat}, {lon})")

            with self.stats_lock:
                self.stats["nominatim_local"] += 1

            norm = normalizar_endereco_completo(raw)
            self.cache_mem[norm] = (lat, lon)
            self.writer.salvar_cache(norm, lat, lon, tipo="pdv")
            return lat, lon, "nominatim_local"

        # ============================================================
        # 6) Google (opcional)
        # ============================================================
        if self.usar_google and self.GOOGLE_KEY:
            try:
                from urllib.parse import quote

                google_url = (
                    "https://maps.googleapis.com/maps/api/geocode/json?"
                    f"address={quote(raw + ', Brasil')}&key={self.GOOGLE_KEY}"
                )

                logger.debug(f"🟡 [GOOGLE] REQUEST → {google_url}")

                r = requests.get(google_url, timeout=5)
                logger.debug(f"🟡 [GOOGLE] RESPONSE → {r.text[:200]}")

                if r.status_code == 200:
                    data = r.json()
                    if data.get("status") == "OK":
                        loc = data["results"][0]["geometry"]["location"]
                        lat, lon = loc["lat"], loc["lng"]

                        with self.stats_lock:
                            self.stats["google"] += 1

                        logger.debug(f"🟡 [GOOGLE] SUCESSO ({lat}, {lon})")
                        return lat, lon, "google"

            except Exception as e:
                logger.warning(f"⚠️ [GOOGLE] Erro: {e}")

        # ============================================================
        # 7) Falha final
        # ============================================================
        with self.stats_lock:
            self.stats["falha"] += 1
        logger.warning(f"❌ [FALHA] Sem coordenadas → '{raw}'")
        return None, None, "falha"



    # ============================================================
    # ⚡ Execução em lote
    # ============================================================
    def geocodificar_em_lote(self, entradas: List[str], tipo: str = "PDV") -> Dict[str, Tuple[float, float, str]]:
        from pdv_preprocessing.domain.utils_texto import fix_encoding
        import re

        if not entradas:
            return {}

        total = len(entradas)
        max_workers = min(self.max_workers, 10 if total < 1000 else 25 if total < 3000 else 40)

        resultados = {}
        inicio_total = time.time()

        logger.info(f"🚀 Geocodificação em lote ({tipo}) iniciada — {total} registros")

        # Reset das estatísticas (com lock para evitar race condition entre threads)
        with self.stats_lock:
            self.stats = {
                "cache_mem": 0,
                "cache_db": 0,
                "nominatim_local": 0,
                "google": 0,
                "falha": 0,
                "total": 0,
            }

        
        def _worker(e_raw):
            e_norm = fix_encoding(e_raw.strip())

            # === Extrair CEP real ===
            cep = None
            m = re.search(r"\b(\d{5})[- ]?(\d{3})\b", e_norm)
            if m:
                cep = f"{m.group(1)}{m.group(2)}"

            # Modo PDV
            if tipo == "PDV":
                return self.buscar_coordenadas(e_norm, cep)

            # Modo MKP (usa o CEP como endereço)
            if tipo == "MKP":
                return self.buscar_coordenadas(None, e_norm)

            return None, None, "tipo_invalido"

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {executor.submit(_worker, e): e for e in entradas}

            for i, futuro in enumerate(as_completed(futuros), 1):
                chave = futuros[futuro]
                try:
                    resultados[chave] = futuro.result()
                except Exception as e:
                    logger.warning(f"⚠️ Erro geocodificando {chave}: {e}")

                if i % 200 == 0 or i == total:
                    logger.info(f"📌 Progresso {i}/{total}")

        dur = time.time() - inicio_total
        logger.info(f"🏁 Finalizado: {len(resultados)}/{total} em {dur:.1f}s")
        return resultados


    # ============================================================
    # 📊 Resumo final
    # ============================================================
    def exibir_resumo_logs(self):
        total = self.stats["total"]
        logger.info("📊 Resumo Geolocalização:")
        for origem, count in self.stats.items():
            if origem != "total":
                pct = (count / total * 100) if total else 0
                logger.info(f"   {origem:<18}: {count:>6} ({pct:5.1f}%)")

        sucesso = total - self.stats["falha"]
        taxa = (sucesso / total * 100) if total else 0
        logger.info(f"✅ Sucesso: {sucesso}/{total} ({taxa:.1f}%)")

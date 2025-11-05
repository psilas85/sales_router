# ============================================================
# 📦 src/pdv_preprocessing/domain/geolocation_service.py
# ============================================================

import os
import time
import requests
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import uniform
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter


class GeolocationService:
    """
    Serviço de georreferenciamento unificado:
      - Cache em memória e banco
      - Fallback: Cache → Nominatim público → Google
      - Execução paralela com retries e backoff
      - Compatível com PDV e MKP
    """

    def __init__(self, reader: DatabaseReader, writer: DatabaseWriter, max_workers: int = 20):
        self.reader = reader
        self.writer = writer
        self.GOOGLE_KEY = os.getenv("GMAPS_API_KEY")
        self.NOMINATIM_PUBLIC = "https://nominatim.openstreetmap.org/search"
        self.timeout = 5
        self.max_workers = max_workers

        self.cache_mem = {}
        self.stats = {
            "cache_mem": 0,
            "cache_db": 0,
            "nominatim_public": 0,
            "google": 0,
            "falha": 0,
            "total": 0,
        }

    # ============================================================
    # 🧭 Coordenadas genéricas conhecidas (para descartar)
    # ============================================================
    def _is_generic_location(self, lat: float, lon: float) -> bool:
        if lat is None or lon is None:
            return True
        pontos_genericos = [
            (-23.5506507, -46.6333824),  # São Paulo
            (-22.908333, -43.196388),    # Rio de Janeiro
            (-15.7801, -47.9292),        # Brasília
            (-19.9167, -43.9345),        # Belo Horizonte
        ]
        for ref_lat, ref_lon in pontos_genericos:
            if abs(lat - ref_lat) < 0.0005 and abs(lon - ref_lon) < 0.0005:
                return True
        return False

    # ============================================================
    # 🌍 Busca coordenadas com fallback inteligente (auto-switch)
    # ============================================================
    def buscar_coordenadas(self, endereco: str | None, cep: str | None = None) -> tuple[float, float, str]:
        if not endereco and not cep:
            logging.warning("⚠️ Chamada de geocodificação com parâmetros vazios.")
            return None, None, "parametro_vazio"

        query = (cep or endereco).strip().lower()
        self.stats["total"] += 1

        # 1️⃣ Cache em memória
        if query in self.cache_mem:
            self.stats["cache_mem"] += 1
            lat, lon = self.cache_mem[query]
            logging.debug(f"📦 [CACHE_MEM] {query} → ({lat}, {lon})")
            return lat, lon, "cache_mem"

        # 2️⃣ Cache no banco
        cache_db = self.reader.buscar_localizacao(endereco) if endereco else self.reader.buscar_localizacao_mkp(cep)
        if cache_db:
            lat, lon = cache_db
            self.stats["cache_db"] += 1
            self.cache_mem[query] = (lat, lon)
            logging.debug(f"🗄️ [CACHE_DB] {query} → ({lat}, {lon})")
            return lat, lon, "cache_db"

        # ============================================================
        # 🚦 Controle adaptativo (modo degradado temporário)
        # ============================================================
        now = time.time()
        if getattr(self, "_modo_google_ativo_ate", 0) > now:
            modo_google_ativo = True
        else:
            modo_google_ativo = False

        # ============================================================
        # 3️⃣ Nominatim público (modo normal)
        # ============================================================
        if not modo_google_ativo:
            headers = {"User-Agent": "SalesRouter-Geocoder/1.0"}
            url_pub = f"{self.NOMINATIM_PUBLIC}?q={query}+Brasil&countrycodes=br&format=json"
            for tent in range(3):
                try:
                    r = requests.get(url_pub, headers=headers, timeout=self.timeout)
                    if r.status_code == 200:
                        dados = r.json()
                        if isinstance(dados, list) and len(dados) > 0:
                            lat, lon = float(dados[0]["lat"]), float(dados[0]["lon"])
                            if not self._is_generic_location(lat, lon):
                                self.stats["nominatim_public"] += 1
                                self.cache_mem[query] = (lat, lon)
                                self.writer.salvar_cache(endereco or cep, lat, lon, tipo="mkp" if cep else "pdv")
                                logging.info(f"🌍 [NOMINATIM] {query} → ({lat}, {lon})")
                                return lat, lon, "nominatim_public"
                            else:
                                logging.warning(f"⚠️ Coordenada genérica descartada para '{query}' → ({lat}, {lon})")
                    elif r.status_code == 429:
                        # Too Many Requests → ativa fallback Google temporário
                        logging.warning("🚦 Nominatim atingiu limite → mudando para modo Google por 2 minutos.")
                        self._modo_google_ativo_ate = now + 120
                        modo_google_ativo = True
                        break
                except Exception as e:
                    logging.warning(f"⚠️ Tentativa {tent+1}/3 falhou no Nominatim → {e}")
                    if "Network is unreachable" in str(e) or "Max retries" in str(e):
                        # Ativa modo Google temporário por 2 minutos
                        self._modo_google_ativo_ate = now + 120
                        modo_google_ativo = True
                        break
                time.sleep(0.8 * (2 ** tent) + uniform(0, 0.3))

        # ============================================================
        # 4️⃣ Google Maps fallback (ou modo degradado ativo)
        # ============================================================
        if modo_google_ativo and self.GOOGLE_KEY:
            from urllib.parse import quote
            url_google = (
                f"https://maps.googleapis.com/maps/api/geocode/json?"
                f"address={quote(query+', Brasil')}&key={self.GOOGLE_KEY}"
            )
            try:
                r = requests.get(url_google, timeout=self.timeout)
                if r.status_code == 200:
                    dados = r.json()
                    if dados.get("status") == "OK" and dados.get("results"):
                        loc = dados["results"][0]["geometry"]["location"]
                        lat, lon = loc["lat"], loc["lng"]
                        if not self._is_generic_location(lat, lon):
                            self.stats["google"] += 1
                            self.cache_mem[query] = (lat, lon)
                            self.writer.salvar_cache(endereco or cep, lat, lon, tipo="mkp" if cep else "pdv")
                            logging.info(f"🗺️ [GOOGLE] {query} → ({lat}, {lon})")
                            return lat, lon, "google"
            except Exception as e:
                logging.warning(f"⚠️ Falha no Google Maps → {e}")

        # ❌ Nenhum resultado
        self.stats["falha"] += 1
        logging.warning(f"💀 Nenhuma coordenada encontrada para '{query}' após 3 tentativas.")
        return None, None, "falha"


    # ============================================================
    # ⚡ Geocodificação em lote (threads com controle adaptativo)
    # ============================================================
    def geocodificar_em_lote(self, entradas: list[str], tipo: str = "PDV") -> dict[str, tuple[float, float, str]]:
        if not entradas:
            return {}

        total = len(entradas)
        # número de threads cresce até o limite máximo, com base no volume
        max_workers = min(self.max_workers, 10 if total < 1000 else 25 if total < 3000 else 40)
        inicio_total = time.time()
        resultados = {}

        logging.info(f"🚀 Geocodificação em lote ({tipo}) iniciada: {total} registros | {max_workers} threads")

        # Pequena espera entre disparos para evitar banimento do Nominatim público
        def _worker(e):
            # delay aleatório de 0.05–0.2s para distribuir carga entre threads
            time.sleep(uniform(0.05, 0.2))
            return self.buscar_coordenadas(e if tipo == 'PDV' else None, e if tipo == 'MKP' else None)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {executor.submit(_worker, e): e for e in entradas}

            for i, futuro in enumerate(as_completed(futuros), 1):
                chave = futuros[futuro]
                try:
                    coords = futuro.result()
                    if coords:
                        resultados[chave] = coords
                except Exception as e:
                    logging.warning(f"⚠️ Erro geocodificando {chave}: {e}")

                # logs intermediários
                if i % 200 == 0 or i == total:
                    resolvidos = len(resultados)
                    falhas = self.stats["falha"]
                    logging.info(
                        f"🧩 Progresso: {i}/{total} ({100 * i / total:.1f}%) "
                        f"→ {resolvidos} resolvidos | {falhas} falhas"
                    )

                    # throttling adaptativo — se falhas >=10%, reduz velocidade
                    if falhas / max(1, i) > 0.1:
                        logging.warning("⚠️ Muitas falhas recentes — aplicando pausa preventiva (3s)...")
                        time.sleep(3)

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
        total = self.stats["total"]
        logging.info("📊 Resumo de Geolocalização:")
        for origem, count in self.stats.items():
            if origem != "total":
                pct = (count / total * 100) if total else 0
                logging.info(f"   {origem:<18}: {count:>6} ({pct:5.1f}%)")
        logging.info(f"   total               : {total:>6}")

        sucesso = total - self.stats["falha"]
        taxa = (sucesso / total * 100) if total else 0
        logging.info(f"✅ Taxa de sucesso: {sucesso}/{total} ({taxa:.1f}%)")


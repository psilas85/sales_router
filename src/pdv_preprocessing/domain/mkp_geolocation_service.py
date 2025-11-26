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

        # Cache e estatísticas de execução
        self.cache_mem: Dict[str, Tuple[float, float]] = {}
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
    @staticmethod
    def _is_generic_location(lat: float, lon: float) -> bool:
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
    # 🌍 Busca coordenadas com fallback inteligente
    # ============================================================
    def buscar_coordenadas(self, endereco: Optional[str], cep: Optional[str] = None) -> Tuple[Optional[float], Optional[float], str]:
        if not endereco and not cep:
            logger.warning("⚠️ Chamada de geocodificação com parâmetros vazios.")
            return None, None, "parametro_vazio"

        query = (cep or endereco).strip().lower()
        self.stats["total"] += 1

        # ============================================================
        # 1️⃣ Cache em memória
        # ============================================================
        if query in self.cache_mem:
            lat, lon = self.cache_mem[query]
            self.stats["cache_mem"] += 1
            logger.debug(f"📦 [CACHE_MEM] {query} → ({lat}, {lon})")
            return lat, lon, "cache_mem"

        # ============================================================
        # 2️⃣ Cache no banco
        # ============================================================
        try:
            cache_db = self.reader.buscar_localizacao(endereco) if endereco else self.reader.buscar_localizacao_mkp(cep)
            if cache_db:
                lat, lon = cache_db
                self.stats["cache_db"] += 1
                self.cache_mem[query] = (lat, lon)
                logger.debug(f"🗄️ [CACHE_DB] {query} → ({lat}, {lon})")
                return lat, lon, "cache_db"
        except Exception as e:
            logger.warning(f"⚠️ Erro ao consultar cache DB: {e}")

        # ============================================================
        # 🚦 Modo Google temporário (se limite Nominatim excedido)
        # ============================================================
        now = time.time()
        modo_google_ativo = getattr(self, "_modo_google_ativo_ate", 0) > now

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
                                logger.debug(f"🌍 [NOMINATIM] {query} → ({lat}, {lon})")
                                return lat, lon, "nominatim_public"
                            else:
                                logger.warning(f"⚠️ Coordenada genérica descartada: {query} → ({lat}, {lon})")

                    elif r.status_code == 429:
                        logger.warning("🚦 Nominatim atingiu limite → mudando para modo Google (2 min).")
                        self._modo_google_ativo_ate = now + 120
                        modo_google_ativo = True
                        break
                except Exception as e:
                    logger.warning(f"⚠️ Tentativa {tent+1}/3 falhou no Nominatim → {e}")
                    if "Network is unreachable" in str(e) or "Max retries" in str(e):
                        self._modo_google_ativo_ate = now + 120
                        modo_google_ativo = True
                        break
                time.sleep(0.8 * (2 ** tent) + uniform(0, 0.3))

        # ============================================================
        # 4️⃣ Google Maps fallback
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
                            logger.debug(f"🗺️ [GOOGLE] {query} → ({lat}, {lon})")
                            return lat, lon, "google"
            except Exception as e:
                logger.warning(f"⚠️ Falha no Google Maps → {e}")

        # ============================================================
        # ❌ Nenhum resultado
        # ============================================================
        self.stats["falha"] += 1
        logger.warning(f"💀 Nenhuma coordenada encontrada para '{query}' após 3 tentativas.")
        return None, None, "falha"

    # ============================================================
    # ⚡ Geocodificação em lote (multithread + adaptativa)
    # ============================================================
    def geocodificar_em_lote(self, entradas: List[str], tipo: str = "PDV") -> Dict[str, Tuple[float, float, str]]:
        if not entradas:
            return {}

        total = len(entradas)
        max_workers = min(self.max_workers, 10 if total < 1000 else 25 if total < 3000 else 40)
        inicio_total = time.time()
        resultados = {}

        logger.info(f"🚀 Geocodificação em lote ({tipo}) iniciada: {total} registros | {max_workers} threads")

        def _worker(e):
            time.sleep(uniform(0.05, 0.2))  # Distribui carga
            return self.buscar_coordenadas(e if tipo == "PDV" else None, e if tipo == "MKP" else None)

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {executor.submit(_worker, e): e for e in entradas}

            for i, futuro in enumerate(as_completed(futuros), 1):
                chave = futuros[futuro]
                try:
                    coords = futuro.result()
                    if coords:
                        resultados[chave] = coords
                except Exception as e:
                    logger.warning(f"⚠️ Erro geocodificando {chave}: {e}")

                if i % 200 == 0 or i == total:
                    resolvidos = len(resultados)
                    falhas = self.stats["falha"]
                    logger.info(
                        f"🧩 Progresso: {i}/{total} ({100 * i / total:.1f}%) "
                        f"→ {resolvidos} resolvidos | {falhas} falhas"
                    )
                    if falhas / max(1, i) > 0.1:
                        logger.warning("⚠️ Muitas falhas recentes — aplicando pausa preventiva (3s)...")
                        time.sleep(3)

        dur = time.time() - inicio_total
        taxa_ok = (len(resultados) / total * 100) if total else 0
        logger.info(
            f"✅ Concluído: {len(resultados)}/{total} resolvidos ({taxa_ok:.1f}%) em {dur:.1f}s "
            f"→ média {dur / total:.2f}s/reg"
        )
        return resultados

    # ============================================================
    # 📊 Resumo de logs
    # ============================================================
    def exibir_resumo_logs(self):
        total = self.stats["total"]
        logger.info("📊 Resumo de Geolocalização:")
        for origem, count in self.stats.items():
            if origem != "total":
                pct = (count / total * 100) if total else 0
                logger.info(f"   {origem:<18}: {count:>6} ({pct:5.1f}%)")
        logger.info(f"   total               : {total:>6}")

        sucesso = total - self.stats["falha"]
        taxa = (sucesso / total * 100) if total else 0
        logger.info(f"✅ Taxa de sucesso: {sucesso}/{total} ({taxa:.1f}%)")








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
from pdv_preprocessing.domain.utils_geo import coordenada_generica, cep_invalido
from geopy.distance import geodesic

# ============================================================
# 🆕 ViaCEP – fallback para CEP sem logradouro
# ============================================================
def tentar_inferir_endereco_via_cep(cep: str) -> Optional[str]:
    try:
        url = f"https://viacep.com.br/ws/{cep}/json/"
        r = requests.get(url, timeout=5)

        if r.status_code != 200:
            return None

        data = r.json()
        if data.get("erro"):
            return None

        cidade = data.get("localidade")
        uf = data.get("uf")
        bairro = data.get("bairro")

        if not cidade or not uf:
            return None

        if bairro:
            return f"{bairro}, {cidade} - {uf}, Brasil"
        return f"{cidade} - {uf}, Brasil"

    except Exception:
        return None


# ============================================================
# 🔍 Detecta coordenadas suspeitas (fallback Nominatim / Google)
# ============================================================
def coordenada_suspeita(lat, lon):
    fallback_nominatim = (-21.993173, -47.3333435)
    if geodesic((lat, lon), fallback_nominatim).km < 5:
        return True

    fallback_google = (-14.235004, -51.92528)
    if geodesic((lat, lon), fallback_google).km < 50:
        return True

    return False


# ============================================================
# 🌎 GeolocationService
# ============================================================
class GeolocationService:

    def __init__(self, reader: DatabaseReader, writer: DatabaseWriter, max_workers: int = 20):
        self.reader = reader
        self.writer = writer
        self.GOOGLE_KEY = os.getenv("GMAPS_API_KEY")
        self.NOMINATIM_PUBLIC = "https://nominatim.openstreetmap.org/search"
        self.timeout = 5
        self.max_workers = max_workers

        # Cache em memória
        self.cache_mem: Dict[str, Tuple[float, float]] = {}

        # Stats
        self.stats = {
            "cache_mem": 0,
            "cache_db": 0,
            "nominatim_public": 0,
            "google": 0,
            "falha": 0,
            "total": 0,
        }

    # ============================================================
    # 🔍 Buscar coordenadas (PDV ou MKP)
    # ============================================================
    def buscar_coordenadas(
        self,
        endereco: Optional[str],
        cep: Optional[str] = None
    ) -> Tuple[Optional[float], Optional[float], str]:

        if not endereco and not cep:
            return None, None, "parametro_vazio"

        query_original = cep or endereco
        query = query_original.strip().lower()
        self.stats["total"] += 1

        logger.info(f"🔎 [GEO] Iniciando busca p/ '{query_original}'")

        # ------------------------------------------------------------
        # 0️⃣ CEP inválido por regra interna
        # ------------------------------------------------------------
        cep_clean = None
        if cep:
            cep_clean = cep.replace("-", "").strip().zfill(8)
            if cep_invalido(cep_clean):
                logger.warning(f"🚫 CEP inválido: {cep_clean}")
                return None, None, "cep_invalido"

        # ------------------------------------------------------------
        # 1️⃣ CACHE EM MEMÓRIA
        # ------------------------------------------------------------
        if query in self.cache_mem:
            lat, lon = self.cache_mem[query]
            return lat, lon, "cache_mem"

        # ------------------------------------------------------------
        # 2️⃣ CACHE NO BANCO
        # ------------------------------------------------------------
        try:
            if endereco:
                cache_db = self.reader.buscar_localizacao(endereco)
            else:
                cache_db = self.reader.buscar_localizacao_mkp(cep_clean)

            if cache_db:
                lat, lon = cache_db
                if not (coordenada_generica(lat, lon) or coordenada_suspeita(lat, lon)):
                    self.cache_mem[query] = (lat, lon)
                    return lat, lon, "cache_db"
        except:
            pass

        # ------------------------------------------------------------
        # 3️⃣ GOOGLE
        # ------------------------------------------------------------
        if cep_clean:
            url_google = (
                "https://maps.googleapis.com/maps/api/geocode/json?"
                f"components=postal_code:{cep_clean}|country:BR&key={self.GOOGLE_KEY}"
            )

            try:
                r = requests.get(url_google, timeout=self.timeout)
                dados = r.json()
                status = dados.get("status", "")

                # ✔ encontrou coordenada
                if status == "OK":
                    loc = dados["results"][0]["geometry"]["location"]
                    lat, lon = loc["lat"], loc["lng"]

                    if not (coordenada_generica(lat, lon) or coordenada_suspeita(lat, lon)):
                        self.writer.salvar_cache(cep_clean, lat, lon, tipo="mkp", fonte="google")
                        self.cache_mem[query] = (lat, lon)
                        return lat, lon, "google"

                # 🚨 ZERO_RESULTS → handled no pipeline de lote
                if status == "ZERO_RESULTS":
                    logger.warning(f"🗺️ [GOOGLE] ZERO_RESULTS p/ {cep_clean}")
                    return None, None, "google_zero"

            except:
                pass

        return None, None, "falha"


    
    # ============================================================
    # ⚡ Geocodificação em lote (MKP + PDV)
    # ============================================================
    def geocodificar_em_lote(self, entradas: List[str], tipo: str = "PDV") \
            -> Dict[str, Tuple[float, float, str]]:

        if not entradas:
            return {}

        total = len(entradas)
        max_workers = min(self.max_workers, 40)
        inicio_total = time.time()
        resultados = {}

        logger.info(
            f"🚀 Geocodificação em lote ({tipo}) iniciada — "
            f"{total} registros | {max_workers} threads"
        )

        headers = {"User-Agent": "SalesRouter-Geocoder/1.0"}


        # ============================================================
        # THREAD WORKER
        # ============================================================
        def _worker(e):
            time.sleep(uniform(0.01, 0.03))
            cep_clean = e.replace("-", "").strip().zfill(8)

            # ========================================================
            # 1️⃣ PRIMEIRA TENTATIVA — buscar_coordenadas()
            #    (cache → banco → Google)
            # ========================================================
            try:
                lat, lon, origem = self.buscar_coordenadas(
                    e if tipo == "PDV" else None,
                    e if tipo == "MKP" else None
                )

                # ✔ sucesso direto
                if lat is not None and lon is not None:
                    return lat, lon, origem

                # 🚨 ZERO_RESULTS → parte 2/4 termina aqui
                # ViaCEP será executado na PARTE 3/4
                if origem == "google_zero":
                    # devolve sinal especial para parte 3
                    return None, None, "google_zero"

            except Exception:
                pass

            # ========================================================
            # 2️⃣ NOMINATIM POSTAL (somente MKP)
            # ========================================================
            if tipo == "MKP":
                try:
                    url_postal = (
                        f"{self.NOMINATIM_PUBLIC}"
                        f"?postalcode={cep_clean}&country=br"
                        f"&format=json&addressdetails=1"
                    )

                    r = requests.get(url_postal, headers=headers, timeout=self.timeout)

                    if r.status_code == 200 and r.json():
                        lat = float(r.json()[0]["lat"])
                        lon = float(r.json()[0]["lon"])

                        if not (coordenada_generica(lat, lon) or coordenada_suspeita(lat, lon)):
                            self.writer.salvar_cache(
                                cep_clean, lat, lon,
                                tipo="mkp",
                                fonte="nominatim_postal"
                            )
                            return lat, lon, "nominatim_postal"
                except:
                    pass

            # ========================================================
            # 3️⃣ NOMINATIM FULL
            # ========================================================
            try:
                url_q = (
                    f"{self.NOMINATIM_PUBLIC}?"
                    f"q={e} Brasil&countrycodes=br"
                    f"&format=json&addressdetails=1"
                )

                r = requests.get(url_q, headers=headers, timeout=self.timeout)

                if r.status_code == 200 and r.json():
                    lat = float(r.json()[0]["lat"])
                    lon = float(r.json()[0]["lon"])

                    if not (coordenada_generica(lat, lon) or coordenada_suspeita(lat, lon)):
                        self.writer.salvar_cache(
                            cep_clean, lat, lon,
                            tipo="mkp",
                            fonte="nominatim_q"
                        )
                        return lat, lon, "nominatim_q"
            except:
                pass
            # ========================================================
            # 4️⃣ GOOGLE FINAL (antes de tentar ViaCEP)
            #    Só prepara a estrutura, mas o ViaCEP verdadeiro
            #    será executado abaixo, caso google_zero
            # ========================================================
            # (nada aqui — continuamos abaixo)


            # ========================================================
            # 🟡 5️⃣ VIA_CEP — Fallback após ZERO_RESULTS no Google
            # ========================================================
            # Só executamos ViaCEP se a PARTE 2 retornou "google_zero"
            if True:  # executa sempre que chegar aqui
                # Se a parte 2 NÃO retornou google_zero, seguimos.
                # Mas se retornou "google_zero", lat/lon/resultado foram:
                # (None, None, "google_zero")
                try:
                    # Verifica explicitamente se a primeira parte sinalizou google_zero
                    lat0, lon0, status0 = self.buscar_coordenadas(
                        None if tipo == "MKP" else e,
                        e if tipo == "MKP" else None
                    )
                    is_zero = (status0 == "google_zero")
                except:
                    is_zero = False

                if is_zero:
                    endereco_inf = tentar_inferir_endereco_via_cep(cep_clean)

                    if endereco_inf:
                        logger.info(f"🟡 [VIA_CEP] Endereço inferido para {cep_clean}: {endereco_inf}")

                        try:
                            url_inf = (
                                f"{self.NOMINATIM_PUBLIC}?q={endereco_inf}"
                                f"&format=json&addressdetails=1&countrycodes=br"
                            )

                            r_inf = requests.get(url_inf, timeout=self.timeout)

                            if r_inf.status_code == 200 and r_inf.json():
                                dados_inf = r_inf.json()[0]

                                lat = float(dados_inf["lat"])
                                lon = float(dados_inf["lon"])

                                # pequena aleatorização para quebrar pontos sobrepostos
                                lat += uniform(-0.00025, 0.00025)
                                lon += uniform(-0.00025, 0.00025)

                                if not (
                                    coordenada_generica(lat, lon)
                                    or coordenada_suspeita(lat, lon)
                                ):
                                    # grava no cache
                                    self.writer.salvar_cache(
                                        cep_clean, lat, lon,
                                        tipo="mkp",
                                        fonte="via_cep_inferido"
                                    )
                                    return lat, lon, "via_cep_inferido"

                        except Exception as ex_vc:
                            logger.warning(f"⚠️ Erro no ViaCEP fallback ({cep_clean}): {ex_vc}")


            # ========================================================
            # 6️⃣ GOOGLE — fallback FINAL
            # ========================================================
            if self.GOOGLE_KEY:
                try:
                    url_google = (
                        "https://maps.googleapis.com/maps/api/geocode/json?"
                        f"components=postal_code:{cep_clean}|country:BR&key={self.GOOGLE_KEY}"
                    )

                    r = requests.get(url_google, timeout=self.timeout)
                    dados = r.json()

                    if dados.get("status") == "OK":
                        loc = dados["results"][0]["geometry"]["location"]
                        lat, lon = loc["lat"], loc["lng"]

                        if not (coordenada_generica(lat, lon) or coordenada_suspeita(lat, lon)):
                            self.writer.salvar_cache(
                                cep_clean, lat, lon,
                                tipo="mkp",
                                fonte="google_fallback"
                            )
                            return lat, lon, "google"

                except Exception as ex_g:
                    logger.warning(f"⚠️ Erro fallback Google final ({cep_clean}): {ex_g}")

            # ========================================================
            # 7️⃣ FALHA TOTAL
            # ========================================================
            return None, None, "falha"


            # ============================================================
        # EXECUÇÃO PARALELA
        # ============================================================
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = {executor.submit(_worker, e): e for e in entradas}

            for i, futuro in enumerate(as_completed(futuros), 1):
                chave = futuros[futuro]

                try:
                    lat, lon, origem = futuro.result()

                    if origem == "cep_invalido":
                        resultados[chave] = (None, None, "cep_invalido")
                        continue

                    if lat is not None and lon is not None:
                        resultados[chave] = (lat, lon, origem)
                    else:
                        self.stats["falha"] += 1

                except Exception as e:
                    logger.warning(f"⚠️ Erro geocodificando {chave}: {e}")
                    self.stats["falha"] += 1

                if i % 200 == 0 or i == total:
                    logger.info(
                        f"🧩 Progresso: {i}/{total} "
                        f"({100 * i/total:.1f}%) | "
                        f"{len(resultados)} OK | "
                        f"{self.stats['falha']} falhas"
                    )

        dur = time.time() - inicio_total
        logger.info(
            f"✅ Concluído: {len(resultados)}/{total} resolvidos "
            f"({100 * len(resultados)/total:.1f}%) em {dur:.1f}s"
        )

        return resultados


    # ============================================================
    # 📊 Resumo final da geolocalização
    # ============================================================
    def exibir_resumo_logs(self):
        total = self.stats["total"]
        logger.info("📊 Resumo de Geolocalização:")

        for origem, count in self.stats.items():
            if origem != "total":
                pct = (count / total * 100) if total else 0
                logger.info(f"   {origem:<18}: {count:>6} ({pct:5.1f}%)")

        logger.info(f"   total               : {total:>6}")

        sucesso = total - self.stats["falha"]
        taxa = (sucesso / total * 100) if total else 0
        logger.info(f"✅ Taxa de sucesso: {sucesso}/{total} ({taxa:.1f}%)")

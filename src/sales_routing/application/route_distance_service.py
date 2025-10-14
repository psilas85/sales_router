# sales_router/src/sales_routing/application/route_distance_service.py

import os
import math
import requests
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime
from loguru import logger

class RouteDistanceService:
    """
    Serviço de cálculo de distância e tempo entre dois pontos (lat, lon)
    com camadas de fallback:
      1️⃣ OSRM local/remoto
      2️⃣ Google Maps Directions API
      3️⃣ Haversine geodésico
    Com cache persistente no banco (tabela route_cache).
    """

    def __init__(self):
        # URLs e tokens
        self.osrm_url = os.getenv("OSRM_URL", "http://osrm:5000")
        self.google_api_key = os.getenv("GMAPS_API_KEY")
        self.alpha_path = float(os.getenv("ALPHA_PATH", 1.3))

        # Conexão PostgreSQL
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "sales_routing_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
        )
        self.conn.autocommit = True

        # Contadores para progresso
        self.req_count = 0
        self.req_cache = 0
        self.req_osrm = 0
        self.req_google = 0
        self.req_haversine = 0

    # ============================================================
    # Função principal
    # ============================================================
    def get_distance_time(self, a: tuple[float, float], b: tuple[float, float]) -> dict:
        """
        Retorna dict com distância (km), tempo (min) e fonte ('osrm', 'google', 'haversine', 'cache')
        """

        fonte = None
        dist_km, tempo_min = None, None

        # 1️⃣ Cache
        cached = self._buscar_cache(a, b)
        if cached:
            fonte = "cache"
            dist_km, tempo_min = cached["distancia_km"], cached["tempo_min"]
            self.req_cache += 1
        else:
            # 2️⃣ OSRM
            try:
                dist_km, tempo_min = self._from_osrm(a, b)
                fonte = "osrm"
                self.req_osrm += 1
            except Exception as e:
                logger.warning(f"⚠️ OSRM falhou ({e}). Tentando Google Maps...")
                # 3️⃣ Google
                if self.google_api_key:
                    try:
                        dist_km, tempo_min = self._from_google(a, b)
                        fonte = "google"
                        self.req_google += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Google falhou ({e}). Usando Haversine...")
                # 4️⃣ Haversine
                if dist_km is None:
                    dist_km = self._haversine_km(a, b) * self.alpha_path
                    tempo_min = (dist_km / 40.0) * 60
                    fonte = "haversine"
                    self.req_haversine += 1

            # Salva no cache
            self._gravar_cache(a, b, dist_km, tempo_min, fonte)

        # Incrementa contagem global
        self.req_count += 1
        if self.req_count % 50 == 0:
            self._log_progresso()

        return {
            "distancia_km": round(dist_km, 3),
            "tempo_min": round(tempo_min, 1),
            "fonte": fonte,
        }

    # ============================================================
    # Logs periódicos de progresso
    # ============================================================
    def _log_progresso(self):
        cache_pct = (self.req_cache / self.req_count) * 100 if self.req_count else 0
        osrm_pct = (self.req_osrm / self.req_count) * 100 if self.req_count else 0
        logger.info(
            f"📊 Rotas processadas: {self.req_count} "
            f"(Cache {cache_pct:.1f}%, OSRM {osrm_pct:.1f}%, "
            f"Google {self.req_google}, Haversine {self.req_haversine})"
        )

    # ============================================================
    # OSRM local
    # ============================================================
    def _from_osrm(self, a, b) -> tuple[float, float]:
        url = f"{self.osrm_url}/route/v1/driving/{a[1]},{a[0]};{b[1]},{b[0]}?overview=false"
        resp = requests.get(url, timeout=3)
        data = resp.json()
        if "routes" not in data or not data["routes"]:
            raise Exception("Sem rota OSRM")
        dist_km = data["routes"][0]["distance"] / 1000
        tempo_min = data["routes"][0]["duration"] / 60
        return dist_km, tempo_min

    # ============================================================
    # Google Maps fallback
    # ============================================================
    def _from_google(self, a, b) -> tuple[float, float]:
        base_url = "https://maps.googleapis.com/maps/api/directions/json"
        params = {
            "origin": f"{a[0]},{a[1]}",
            "destination": f"{b[0]},{b[1]}",
            "key": self.google_api_key,
        }
        resp = requests.get(base_url, params=params, timeout=5)
        data = resp.json()
        if data.get("status") != "OK":
            raise Exception(data.get("status", "Erro Google"))
        route = data["routes"][0]["legs"][0]
        dist_km = route["distance"]["value"] / 1000
        tempo_min = route["duration"]["value"] / 60
        return dist_km, tempo_min

    # ============================================================
    # Haversine final
    # ============================================================
    def _haversine_km(self, a, b) -> float:
        R = 6371.0
        lat1, lon1 = map(math.radians, a)
        lat2, lon2 = map(math.radians, b)
        dlat, dlon = lat2 - lat1, lon2 - lon1
        h = math.sin(dlat / 2) ** 2 + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        return 2 * R * math.asin(math.sqrt(h))

    # ============================================================
    # Cache
    # ============================================================
    def _buscar_cache(self, a, b):
        with self.conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT distancia_km, tempo_min, fonte
                FROM route_cache
                WHERE origem_lat = %s AND origem_lon = %s
                  AND destino_lat = %s AND destino_lon = %s;
            """, (a[0], a[1], b[0], b[1]))
            row = cur.fetchone()
            if row:
                return {"distancia_km": row["distancia_km"], "tempo_min": row["tempo_min"]}
        return None

    def _gravar_cache(self, a, b, dist_km, tempo_min, fonte):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO route_cache (
                    origem_lat, origem_lon, destino_lat, destino_lon,
                    distancia_km, tempo_min, fonte, atualizado_em
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (origem_lat, origem_lon, destino_lat, destino_lon) DO NOTHING;
            """, (a[0], a[1], b[0], b[1], dist_km, tempo_min, fonte, datetime.now()))
        logger.debug(f"💾 Cache atualizado ({fonte}) {a} → {b}: {dist_km:.2f} km / {tempo_min:.1f} min")

    # ============================================================
    # Fechamento
    # ============================================================
    def close(self):
        if self.conn:
            logger.info(
                f"🏁 Encerrando DistanceService — Total: {self.req_count}, "
                f"Cache: {self.req_cache}, OSRM: {self.req_osrm}, "
                f"Google: {self.req_google}, Haversine: {self.req_haversine}"
            )
            self.conn.close()

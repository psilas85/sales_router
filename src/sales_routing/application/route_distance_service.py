#sales_router/src/sales_routing/application/route_distance_service.py

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
      1️⃣ OSRM local/remoto (com rota real)
      2️⃣ Google Maps Directions API
      3️⃣ Haversine geodésico
    Com cache persistente no banco (tabela route_cache).
    """

    def __init__(self):
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

        # Contadores de requisições
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
        Retorna dict com distância (km), tempo (min), rota_coord e fonte.
        """

        fonte = None
        dist_km, tempo_min, rota_coord = None, None, []

        # 1️⃣ Cache
        cached = self._buscar_cache(a, b)
        if cached:
            fonte = "cache"
            dist_km, tempo_min = cached["distancia_km"], cached["tempo_min"]
            # ✅ tenta recuperar rota_coord se existir no cache
            rota_coord = cached.get("rota_coord", [])
            if not rota_coord:
                # fallback mínimo se o cache antigo não tiver rota
                rota_coord = [{"lat": a[0], "lon": a[1]}, {"lat": b[0], "lon": b[1]}]
            self.req_cache += 1

        else:
            # 2️⃣ OSRM
            try:
                dist_km, tempo_min, rota_coord = self._from_osrm(a, b)
                fonte = "osrm"
                self.req_osrm += 1
            except Exception as e:
                logger.warning(f"⚠️ OSRM falhou ({e}). Tentando Google Maps...")
                # 3️⃣ Google
                if self.google_api_key:
                    try:
                        dist_km, tempo_min, rota_coord = self._from_google(a, b)
                        fonte = "google"
                        self.req_google += 1
                    except Exception as e:
                        logger.warning(f"⚠️ Google falhou ({e}). Usando Haversine...")
                # 4️⃣ Haversine
                if dist_km is None:
                    dist_km = self._haversine_km(a, b) * self.alpha_path
                    tempo_min = (dist_km / 40.0) * 60
                    rota_coord = [{"lat": a[0], "lon": a[1]}, {"lat": b[0], "lon": b[1]}]
                    fonte = "haversine"
                    self.req_haversine += 1

            # Salva no cache
            self._gravar_cache(a, b, dist_km, tempo_min, fonte)

        # Log periódico
        self.req_count += 1
        if self.req_count % 50 == 0:
            self._log_progresso()

        return {
            "distancia_km": round(dist_km, 3),
            "tempo_min": round(tempo_min, 1),
            "rota_coord": rota_coord,
            "fonte": fonte,
        }

    # ============================================================
    # OSRM local (com geometria real)
    # ============================================================
    def _from_osrm(self, a, b) -> tuple[float, float, list]:
        if None in a or None in b:
            raise ValueError(f"Coordenadas inválidas: {a}, {b}")

        url = f"{self.osrm_url}/route/v1/driving/{a[1]},{a[0]};{b[1]},{b[0]}?overview=full&geometries=geojson"
        resp = requests.get(url, timeout=6)
        data = resp.json()

        if data.get("code") != "Ok" or not data.get("routes"):
            raise Exception(f"Sem rota OSRM válida para {a} → {b}")

        route = data["routes"][0]
        dist_km = route["distance"] / 1000
        tempo_min = route["duration"] / 60

        # ✅ converte corretamente de [lon, lat] para dicts {lat, lon}
        coords = route["geometry"].get("coordinates", [])
        rota_coord = [{"lat": lat, "lon": lon} for lon, lat in coords]

        logger.debug(f"📍 OSRM rota gerada: {len(rota_coord)} pontos / {dist_km:.2f} km / {tempo_min:.1f} min")

        return dist_km, tempo_min, rota_coord


    # ============================================================
    # Google Maps Directions
    # ============================================================
    def _from_google(self, a, b) -> tuple[float, float, list]:
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

        leg = data["routes"][0]["legs"][0]
        dist_km = leg["distance"]["value"] / 1000
        tempo_min = leg["duration"]["value"] / 60
        coords = [{"lat": a[0], "lon": a[1]}, {"lat": b[0], "lon": b[1]}]
        return dist_km, tempo_min, coords

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
                SELECT distancia_km, tempo_min, rota_coord
                FROM route_cache
                WHERE origem_lat = %s AND origem_lon = %s
                AND destino_lat = %s AND destino_lon = %s;
            """, (a[0], a[1], b[0], b[1]))
            return cur.fetchone()


    def _gravar_cache(self, a, b, dist_km, tempo_min, fonte):
        with self.conn.cursor() as cur:
            cur.execute("""
                INSERT INTO route_cache (
                    origem_lat, origem_lon, destino_lat, destino_lon,
                    distancia_km, tempo_min, fonte, atualizado_em
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (origem_lat, origem_lon, destino_lat, destino_lon) DO NOTHING;
            """, (a[0], a[1], b[0], b[1], dist_km, tempo_min, fonte, datetime.now()))
        logger.debug(f"💾 Cache atualizado ({fonte}) {a} → {b}: {dist_km:.2f} km / {tempo_min:.1f} min")

    # ============================================================
    # NOVO: cálculo de rota completa com múltiplos pontos
    # ============================================================
    def get_full_route(self, coords_list: list[tuple[float, float]]) -> dict:
        """
        Calcula rota completa passando por todos os pontos (multi-stop)
        e retorna distância total (km), tempo total (min) e rota_coord real.
        """
        if not coords_list or len(coords_list) < 2:
            raise ValueError("Lista de coordenadas insuficiente para rota completa.")

        # Monta string de coordenadas para o OSRM
        coords_str = ";".join([f"{lon},{lat}" for lat, lon in coords_list])
        url = f"{self.osrm_url}/route/v1/driving/{coords_str}?overview=full&geometries=geojson"
        resp = requests.get(url, timeout=12)
        data = resp.json()

        if data.get("code") != "Ok" or not data.get("routes"):
            raise Exception("OSRM não retornou rota válida.")

        route = data["routes"][0]
        dist_km = route["distance"] / 1000
        tempo_min = route["duration"] / 60

        coords = [{"lat": lat, "lon": lon} for lon, lat in route["geometry"]["coordinates"]]

        logger.info(f"🗺️ Rota completa OSRM: {len(coords)} pontos / {dist_km:.2f} km / {tempo_min:.1f} min")
        return {"distancia_km": dist_km, "tempo_min": tempo_min, "rota_coord": coords}


    # ============================================================
    # Fechamento
    # ============================================================
    def _log_progresso(self):
        cache_pct = (self.req_cache / self.req_count) * 100 if self.req_count else 0
        osrm_pct = (self.req_osrm / self.req_count) * 100 if self.req_count else 0
        logger.info(
            f"📊 Rotas processadas: {self.req_count} "
            f"(Cache {cache_pct:.1f}%, OSRM {osrm_pct:.1f}%, "
            f"Google {self.req_google}, Haversine {self.req_haversine})"
        )

    def close(self):
        if self.conn:
            logger.info(
                f"🏁 Encerrando DistanceService — Total: {self.req_count}, "
                f"Cache: {self.req_cache}, OSRM: {self.req_osrm}, "
                f"Google: {self.req_google}, Haversine: {self.req_haversine}"
            )
            self.conn.close()

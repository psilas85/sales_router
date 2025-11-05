# src/sales_routing/application/route_distance_service.py

# ============================================================
# 📦 src/sales_routing/application/route_distance_service.py
# ============================================================

import os
import json
import math
import requests
import psycopg2
import polyline
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
    Inclui cache persistente (tabela route_cache) e agregação de estatísticas.
    """

    def __init__(self, v_kmh: float = None, alpha_path: float = None):
        # ============================================================
        # ⚙️ Parâmetros operacionais configuráveis
        # ============================================================
        self.v_kmh = v_kmh or float(os.getenv("VEL_KMH", 30.0))
        self.alpha_path = alpha_path or float(os.getenv("ALPHA_PATH", 1.3))

        # ============================================================
        # 🌐 Serviços externos
        # ============================================================
        self.osrm_url = os.getenv("OSRM_URL", "http://osrm:5000")
        self.google_api_key = os.getenv("GMAPS_API_KEY")

        # ============================================================
        # 🗄️ Conexão PostgreSQL
        # ============================================================
        self.conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "sales_routing_db"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "postgres"),
            host=os.getenv("DB_HOST", "localhost"),
            port=os.getenv("DB_PORT", "5432"),
        )
        self.conn.autocommit = True

        # Contadores
        self.req_count = 0
        self.req_cache = 0
        self.req_osrm = 0
        self.req_google = 0
        self.req_haversine = 0

        # Log inicial
        logger.info(
            f"⚙️ RouteDistanceService inicializado | v_kmh={self.v_kmh} km/h | α={self.alpha_path} | OSRM={self.osrm_url}"
        )

    # ============================================================
    # Função principal (par a par)
    # ============================================================
    def get_distance_time(self, a: tuple[float, float], b: tuple[float, float]) -> dict:
        """
        Retorna dict com distância (km), tempo (min), rota_coord e fonte.
        Usa cache quando disponível. Caso contrário, aplica fallback em cascata.
        """
        fonte = None
        dist_km, tempo_min, rota_coord = None, None, []

        # ============================================================
        # 1️⃣ Cache
        # ============================================================
        cached = self._buscar_cache(a, b)
        if cached:
            fonte = "cache"
            dist_km, tempo_min = cached["distancia_km"], cached["tempo_min"]
            rota_coord = cached.get("rota_coord") or [
                {"lat": a[0], "lon": a[1]},
                {"lat": b[0], "lon": b[1]},
            ]
            self.req_cache += 1

        else:
            # ============================================================
            # 2️⃣ OSRM
            # ============================================================
            try:
                dist_km, tempo_min, rota_coord = self._from_osrm(a, b)
                fonte = "osrm"
                self.req_osrm += 1
            except Exception as e:
                logger.warning(f"⚠️ OSRM falhou ({e}). Tentando Google Maps...")

                # ============================================================
                # 3️⃣ Google
                # ============================================================
                if self.google_api_key:
                    try:
                        dist_km, tempo_min, rota_coord = self._from_google(a, b)
                        fonte = "google"
                        self.req_google += 1
                    except Exception as e2:
                        logger.warning(f"⚠️ Google falhou ({e2}). Usando Haversine...")

                # ============================================================
                # 4️⃣ Haversine (último recurso)
                # ============================================================
                if dist_km is None:
                    v_kmh = self.v_kmh or float(os.getenv("VEL_KMH", 30.0))
                    dist_km = self._haversine_km(a, b) * self.alpha_path
                    tempo_min = (dist_km / v_kmh) * 60
                    rota_coord = [
                        {"lat": a[0], "lon": a[1]},
                        {"lat": b[0], "lon": b[1]},
                    ]
                    fonte = "haversine"
                    self.req_haversine += 1

            # ============================================================
            # 💾 Atualiza cache
            # ============================================================
            self._gravar_cache(a, b, dist_km, tempo_min, fonte, rota_coord)


        # ============================================================
        # 🧮 Log periódico
        # ============================================================
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
    # OSRM local (com geometria real e fallback seguro)
    # ============================================================
    def _from_osrm(self, a, b) -> tuple[float, float, list]:
        if None in a or None in b:
            raise ValueError(f"Coordenadas inválidas: {a}, {b}")

        # OSRM espera ordem lon,lat
        url = f"{self.osrm_url}/route/v1/driving/{a[1]},{a[0]};{b[1]},{b[0]}?overview=full&geometries=geojson"

        try:
            resp = requests.get(url, timeout=6)
            data = resp.json()
        except Exception as e:
            raise Exception(f"Falha de conexão com OSRM ({e})")

        if data.get("code") != "Ok" or not data.get("routes"):
            raise Exception(f"Sem rota OSRM válida para {a} → {b}")

        route = data["routes"][0]
        dist_km = route.get("distance", 0) / 1000
        tempo_min = route.get("duration", 0) / 60

        if dist_km < 0.05 or tempo_min < 0.05:
            raise Exception(
                f"OSRM retornou rota nula ({dist_km:.2f} km / {tempo_min:.1f} min) — provável fora da área do mapa."
            )

        coords = [{"lat": lat, "lon": lon} for lon, lat in route["geometry"]["coordinates"]]
        logger.debug(f"📍 OSRM rota: {len(coords)} pts / {dist_km:.2f} km / {tempo_min:.1f} min")

        return dist_km, tempo_min, coords

    # ============================================================
    # Google Maps Directions (fallback secundário)
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

        route = data["routes"][0]
        leg = route["legs"][0]
        dist_km = leg["distance"]["value"] / 1000
        tempo_min = leg["duration"]["value"] / 60

        # 🔹 Decodifica o polyline da rota (geometria real da via)
        coords = []
        try:
            points = route.get("overview_polyline", {}).get("points")
            if points:
                decoded = polyline.decode(points)
                coords = [{"lat": lat, "lon": lon} for lat, lon in decoded]
            else:
                # Fallback simples caso o polyline não venha
                coords = [{"lat": a[0], "lon": a[1]}, {"lat": b[0], "lon": b[1]}]
        except Exception as e:
            logger.warning(f"⚠️ Falha ao decodificar polyline Google: {e}")
            coords = [{"lat": a[0], "lon": a[1]}, {"lat": b[0], "lon": b[1]}]

        logger.debug(
            f"📍 Google rota: {dist_km:.2f} km / {tempo_min:.1f} min | pts={len(coords)}"
        )
        return dist_km, tempo_min, coords


    # ============================================================
    # Haversine (último recurso)
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
            row = cur.fetchone()
            if row and row.get("rota_coord"):
                try:
                    # 🔹 Decodifica JSONB armazenado no Postgres
                    row["rota_coord"] = json.loads(row["rota_coord"]) if isinstance(row["rota_coord"], str) else row["rota_coord"]
                except Exception:
                    row["rota_coord"] = []
            return row

 

    def _gravar_cache(self, a, b, dist_km, tempo_min, fonte, rota_coord=None):
        try:
            with self.conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO route_cache (
                        origem_lat, origem_lon, destino_lat, destino_lon,
                        distancia_km, tempo_min, fonte, atualizado_em, rota_coord
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (origem_lat, origem_lon, destino_lat, destino_lon)
                    DO UPDATE SET
                        distancia_km = EXCLUDED.distancia_km,
                        tempo_min = EXCLUDED.tempo_min,
                        fonte = EXCLUDED.fonte,
                        atualizado_em = EXCLUDED.atualizado_em,
                        rota_coord = EXCLUDED.rota_coord;
                """, (
                    a[0], a[1], b[0], b[1],
                    dist_km, tempo_min, fonte, datetime.now(),
                    json.dumps(rota_coord or [])
                ))
            logger.debug(f"💾 Cache atualizado ({fonte}) {a} → {b}: {dist_km:.2f} km / {tempo_min:.1f} min")
        except Exception as e:
            logger.warning(f"⚠️ Falha ao gravar cache para {a}->{b}: {e}")



    # ============================================================
    # Rota completa multi-stop (OSRM → Google → Haversine)
    # ============================================================
    def get_full_route(self, coords_list: list[tuple[float, float]]) -> dict:
        """
        Calcula rota completa multi-stop com fallback hierárquico.
        """
        if not coords_list or len(coords_list) < 2:
            logger.warning("⚠️ Lista de coordenadas insuficiente para rota completa.")
            return {
                "distancia_km": 0.0,
                "tempo_min": 10.0,
                "rota_coord": [{"lat": c[0], "lon": c[1]} for c in coords_list],
                "fonte": "local",
            }

        # ============================================================
        # 1️⃣ OSRM
        # ============================================================
        try:
            coords_str = ";".join([f"{lon},{lat}" for (lat, lon) in coords_list])
            url = f"{self.osrm_url}/route/v1/driving/{coords_str}?overview=full&geometries=geojson"
            resp = requests.get(url, timeout=12)
            data = resp.json()

            if data.get("code") != "Ok" or not data.get("routes"):
                raise Exception(f"OSRM retornou código inválido ({data.get('code')}).")

            route = data["routes"][0]
            dist_km = route["distance"] / 1000
            tempo_min = route["duration"] / 60
            coords = [{"lat": lat, "lon": lon} for lon, lat in route["geometry"]["coordinates"]]
            fonte = "osrm"

            if dist_km < 0.05 or tempo_min < 0.05:
                raise Exception(f"Rota nula detectada ({dist_km:.2f} km, {tempo_min:.2f} min).")

            logger.debug(f"🗺️ OSRM: {len(coords)} pts / {dist_km:.2f} km / {tempo_min:.1f} min")
            return {"distancia_km": dist_km, "tempo_min": tempo_min, "rota_coord": coords, "fonte": fonte}

        except Exception as e:
            logger.warning(f"⚠️ Falha ao gerar rota via OSRM ({e}). Aplicando fallback...")

        # ============================================================
        # 2️⃣ Google Fallback
        # ============================================================
        if getattr(self, "google_api_key", None):
            try:
                total_km, total_min, coords = 0.0, 0.0, []
                for i in range(len(coords_list) - 1):
                    a, b = coords_list[i], coords_list[i + 1]
                    dist_km, tempo_parcial, _ = self._from_google(a, b)
                    total_km += dist_km
                    total_min += tempo_parcial
                    coords.append({"lat": a[0], "lon": a[1]})
                coords.append({"lat": coords_list[-1][0], "lon": coords_list[-1][1]})
                logger.debug(f"🧭 Google fallback: {total_km:.2f} km / {total_min:.1f} min")
                return {"distancia_km": total_km, "tempo_min": total_min, "rota_coord": coords, "fonte": "google"}
            except Exception as e2:
                logger.warning(f"⚠️ Falha também no Google ({e2}). Usando Haversine...")

        # ============================================================
        # 3️⃣ Haversine Fallback (multi-stop incremental)
        # ============================================================
        v_kmh = self.v_kmh or float(os.getenv("VEL_KMH", 30.0))
        total_km, total_min, coords = 0.0, 0.0, []

        for i in range(len(coords_list) - 1):
            a, b = coords_list[i], coords_list[i + 1]
            dist_km = self._haversine_km(a, b) * getattr(self, "alpha_path", 1.0)
            tempo_parcial = (dist_km / v_kmh) * 60
            total_km += dist_km
            total_min += tempo_parcial
            coords.append({"lat": a[0], "lon": a[1]})

        coords.append({"lat": coords_list[-1][0], "lon": coords_list[-1][1]})
        fonte = "haversine"

        logger.debug(
            f"🧭 Haversine fallback: {total_km:.2f} km / {total_min:.1f} min "
            f"(vel={v_kmh:.1f} km/h, α={getattr(self, 'alpha_path', 1.0)})"
        )

        return {
            "distancia_km": total_km,
            "tempo_min": total_min,
            "rota_coord": coords,
            "fonte": fonte,
        }

    # ============================================================
    # Logs e fechamento
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
        fora_area = getattr(self, "req_out_of_area", 0)
        if self.conn:
            logger.info(
                f"🏁 Encerrando DistanceService — "
                f"Total: {self.req_count}, Cache: {self.req_cache}, OSRM: {self.req_osrm}, "
                f"Google: {self.req_google}, Haversine: {self.req_haversine}, ForaMapa: {fora_area}"
            )
            self.conn.close()

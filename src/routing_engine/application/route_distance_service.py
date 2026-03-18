#sales_router/src/routing_engine/application/route_distance_service.py

from __future__ import annotations

import os
import math
import requests
import polyline

from loguru import logger

from routing_engine.infrastructure.database_reader import DatabaseReader
from routing_engine.infrastructure.database_writer import DatabaseWriter


class RouteDistanceService:
    """
    Camadas:
    1. cache
    2. OSRM
    3. Google Directions
    4. Haversine
    """

    def __init__(self, v_kmh: float | None = None, alpha_path: float | None = None):
        self.v_kmh = float(v_kmh or os.getenv("VEL_KMH", 30.0))
        self.alpha_path = float(alpha_path or os.getenv("ALPHA_PATH", 1.3))

        self.osrm_url = os.getenv("OSRM_URL", "http://osrm:5000")
        self.google_api_key = os.getenv("GMAPS_API_KEY")

        self.reader = DatabaseReader()
        self.writer = DatabaseWriter()

        self.req_count = 0
        self.req_cache = 0
        self.req_osrm = 0
        self.req_google = 0
        self.req_haversine = 0

        logger.info(
            f"⚙️ RouteDistanceService inicializado | v_kmh={self.v_kmh} | alpha={self.alpha_path} | osrm={self.osrm_url}"
        )

    def get_distance_time(
        self,
        a: tuple[float, float],
        b: tuple[float, float],
    ) -> dict:
        fonte = None
        dist_km = None
        tempo_min = None
        rota_coord = []

        cached = self.reader.buscar_route_cache(a[0], a[1], b[0], b[1])
        if cached:
            fonte = "cache"
            dist_km = float(cached["distancia_km"])
            tempo_min = float(cached["tempo_min"])
            rota_coord = cached.get("rota_coord") or [
                {"lat": a[0], "lon": a[1]},
                {"lat": b[0], "lon": b[1]},
            ]
            self.req_cache += 1
        else:
            try:
                dist_km, tempo_min, rota_coord = self._from_osrm(a, b)
                fonte = "osrm"
                self.req_osrm += 1
            except Exception as e:
                logger.warning(f"⚠️ OSRM falhou ({e}). Tentando Google...")

                if self.google_api_key:
                    try:
                        dist_km, tempo_min, rota_coord = self._from_google(a, b)
                        fonte = "google"
                        self.req_google += 1
                    except Exception as e2:
                        logger.warning(f"⚠️ Google falhou ({e2}). Usando Haversine...")

                if dist_km is None:
                    dist_km = self._haversine_km(a, b) * self.alpha_path
                    tempo_min = (dist_km / self.v_kmh) * 60
                    rota_coord = [
                        {"lat": a[0], "lon": a[1]},
                        {"lat": b[0], "lon": b[1]},
                    ]
                    fonte = "haversine"
                    self.req_haversine += 1

            self.writer.gravar_route_cache(
                origem_lat=a[0],
                origem_lon=a[1],
                destino_lat=b[0],
                destino_lon=b[1],
                distancia_km=dist_km,
                tempo_min=tempo_min,
                fonte=fonte,
                rota_coord=rota_coord,
            )

        self.req_count += 1
        if self.req_count % 50 == 0:
            self._log_progresso()

        return {
            "distancia_km": round(float(dist_km), 3),
            "tempo_min": round(float(tempo_min), 1),
            "rota_coord": rota_coord,
            "fonte": fonte,
        }

    def _from_osrm(
        self,
        a: tuple[float, float],
        b: tuple[float, float],
    ) -> tuple[float, float, list]:
        url = (
            f"{self.osrm_url}/route/v1/driving/"
            f"{a[1]},{a[0]};{b[1]},{b[0]}?overview=full&geometries=geojson"
        )

        resp = requests.get(url, timeout=6)
        data = resp.json()

        if data.get("code") != "Ok" or not data.get("routes"):
            raise Exception(f"OSRM sem rota válida para {a} -> {b}")

        route = data["routes"][0]
        dist_km = route["distance"] / 1000
        tempo_min = route["duration"] / 60

        if dist_km < 0.05 or tempo_min < 0.05:
            raise Exception(
                f"Rota nula OSRM ({dist_km:.2f} km / {tempo_min:.2f} min)"
            )

        coords = [
            {"lat": lat, "lon": lon}
            for lon, lat in route["geometry"]["coordinates"]
        ]

        return dist_km, tempo_min, coords

    def _from_google(
        self,
        a: tuple[float, float],
        b: tuple[float, float],
    ) -> tuple[float, float, list]:
        url = "https://maps.googleapis.com/maps/api/directions/json"
        params = {
            "origin": f"{a[0]},{a[1]}",
            "destination": f"{b[0]},{b[1]}",
            "key": self.google_api_key,
        }

        resp = requests.get(url, params=params, timeout=6)
        data = resp.json()

        if data.get("status") != "OK":
            raise Exception(data.get("status", "Erro Google"))

        route = data["routes"][0]
        leg = route["legs"][0]

        dist_km = leg["distance"]["value"] / 1000
        tempo_min = leg["duration"]["value"] / 60

        coords = []
        try:
            encoded = route.get("overview_polyline", {}).get("points")
            if encoded:
                decoded = polyline.decode(encoded)
                coords = [{"lat": lat, "lon": lon} for lat, lon in decoded]
            else:
                coords = [
                    {"lat": a[0], "lon": a[1]},
                    {"lat": b[0], "lon": b[1]},
                ]
        except Exception:
            coords = [
                {"lat": a[0], "lon": a[1]},
                {"lat": b[0], "lon": b[1]},
            ]

        return dist_km, tempo_min, coords

    def _haversine_km(
        self,
        a: tuple[float, float],
        b: tuple[float, float],
    ) -> float:
        r = 6371.0
        lat1, lon1 = map(math.radians, a)
        lat2, lon2 = map(math.radians, b)
        dlat = lat2 - lat1
        dlon = lon2 - lon1

        h = (
            math.sin(dlat / 2) ** 2
            + math.cos(lat1) * math.cos(lat2) * math.sin(dlon / 2) ** 2
        )
        return 2 * r * math.asin(math.sqrt(h))

    def get_stats(self) -> dict:
        return {
            "req_count": int(self.req_count),
            "cache_hits": int(self.req_cache),
            "osrm_hits": int(self.req_osrm),
            "google_hits": int(self.req_google),
            "haversine_hits": int(self.req_haversine),
        }

    def _log_progresso(self):
        cache_pct = (self.req_cache / self.req_count * 100) if self.req_count else 0
        osrm_pct = (self.req_osrm / self.req_count * 100) if self.req_count else 0

        logger.info(
            f"📊 Rotas processadas={self.req_count} | "
            f"cache={cache_pct:.1f}% | osrm={osrm_pct:.1f}% | "
            f"google={self.req_google} | haversine={self.req_haversine}"
        )

    def close(self):
        try:
            self.reader.close()
        except Exception:
            pass

        try:
            self.writer.close()
        except Exception:
            pass
#sales_router/src/routing_engine/application/route_optimizer.py

from __future__ import annotations

from geopy.distance import geodesic
from loguru import logger

from routing_engine.application.route_distance_service import RouteDistanceService


class RouteOptimizer:
    def __init__(
        self,
        v_kmh: float,
        service_min: float,
        alpha_path: float,
        distance_service: RouteDistanceService | None = None,
    ):
        self.v_kmh = float(v_kmh)
        self.service_min = float(service_min)
        self.alpha_path = float(alpha_path)
        self.distance_service = distance_service or RouteDistanceService(
            v_kmh=v_kmh,
            alpha_path=alpha_path,
        )

    def _normalizar_coord(self, lat, lon):
        if lat is None or lon is None:
            return None, None

        lat = float(lat)
        lon = float(lon)

        if abs(lat) > abs(lon):
            return lon, lat

        return lat, lon

    def calcular_rota(self, centro: dict, pdvs: list[dict], aplicar_two_opt: bool = False) -> dict:
        if not pdvs:
            return {
                "sequencia": [],
                "distancia_total_km": 0.0,
                "tempo_total_min": 0.0,
                "rota_coord": [],
            }

        logger.info(
            f"🚦 Calculando rota | PDVs={len(pdvs)} | centro=({centro['lat']}, {centro['lon']})"
        )

        rota = self.nearest_neighbor(centro, pdvs)

        if aplicar_two_opt:
            rota = self.two_opt(rota)

        pontos = [centro] + rota + [centro]
        rota_coords = []
        total_km = 0.0
        total_min = 0.0
        fallback_usado = False
        warnings = 0

        for i in range(len(pontos) - 1):
            a = pontos[i]
            b = pontos[i + 1]

            lat_a, lon_a = self._normalizar_coord(a["lat"], a["lon"])
            lat_b, lon_b = self._normalizar_coord(b["lat"], b["lon"])

            if lat_a is None or lat_b is None:
                continue

            if abs(lat_a - lat_b) < 1e-5 and abs(lon_a - lon_b) < 1e-5:
                warnings += 1
                total_min += self.service_min
                continue

            try:
                result = self.distance_service.get_distance_time(
                    (lat_a, lon_a),
                    (lat_b, lon_b),
                )

                total_km += result["distancia_km"]
                total_min += result["tempo_min"]

                coords_segmento = result.get("rota_coord", [])
                if coords_segmento:
                    rota_coords.extend(
                        [{"lat": c["lat"], "lon": c["lon"]} for c in coords_segmento]
                    )
                else:
                    rota_coords.append({"lat": lat_a, "lon": lon_a})
                    rota_coords.append({"lat": lat_b, "lon": lon_b})
                    fallback_usado = True

            except Exception as e:
                logger.warning(
                    f"⚠️ Fallback geodesic entre ({lat_a},{lon_a}) e ({lat_b},{lon_b}) | {e}"
                )
                total_km += geodesic((lat_a, lon_a), (lat_b, lon_b)).km * self.alpha_path
                rota_coords.append({"lat": lat_a, "lon": lon_a})
                rota_coords.append({"lat": lat_b, "lon": lon_b})
                fallback_usado = True

        if not rota_coords:
            rota_coords = (
                [{"lat": centro["lat"], "lon": centro["lon"]}]
                + [{"lat": p["lat"], "lon": p["lon"]} for p in rota]
                + [{"lat": centro["lat"], "lon": centro["lon"]}]
            )
            fallback_usado = True
        else:
            first = rota_coords[0]
            last = rota_coords[-1]

            if (
                abs(first["lat"] - centro["lat"]) > 1e-4
                or abs(first["lon"] - centro["lon"]) > 1e-4
            ):
                rota_coords.insert(0, {"lat": centro["lat"], "lon": centro["lon"]})

            if (
                abs(last["lat"] - centro["lat"]) > 1e-4
                or abs(last["lon"] - centro["lon"]) > 1e-4
            ):
                rota_coords.append({"lat": centro["lat"], "lon": centro["lon"]})

        total_min += len(pdvs) * self.service_min

        if warnings > 0:
            logger.warning(f"⚠️ {warnings} pares consecutivos com coordenadas idênticas tratados.")

        if fallback_usado:
            logger.warning("⚠️ Rota gerada parcialmente via fallback.")

        logger.success(
            f"✅ Rota concluída | PDVs={len(pdvs)} | dist={round(total_km,2)} km | tempo={round(total_min,1)} min"
        )

        return {
            "sequencia": rota,
            "distancia_total_km": round(total_km, 2),
            "tempo_total_min": round(total_min, 1),
            "rota_coord": rota_coords,
        }

    def _dist_km(self, p1, p2):
        lat1, lon1 = self._normalizar_coord(p1["lat"], p1["lon"])
        lat2, lon2 = self._normalizar_coord(p2["lat"], p2["lon"])

        if lat1 is None or lat2 is None:
            return 0.0

        if abs(lat1 - lat2) < 1e-5 and abs(lon1 - lon2) < 1e-5:
            return 0.0

        try:
            result = self.distance_service.get_distance_time((lat1, lon1), (lat2, lon2))
            return float(result["distancia_km"])
        except Exception:
            return geodesic((lat1, lon1), (lat2, lon2)).km * self.alpha_path

    def nearest_neighbor(self, centro: dict, pdvs: list[dict]) -> list[dict]:
        if not pdvs:
            return []

        nao_visitados = pdvs.copy()
        rota = []
        atual = centro

        while nao_visitados:
            prox = min(nao_visitados, key=lambda p: self._dist_km(atual, p))
            rota.append(prox)
            nao_visitados.remove(prox)
            atual = prox

        return rota

    def two_opt(self, rota: list[dict]) -> list[dict]:
        melhor = rota
        melhor_dist = self._total_dist_km(rota)
        melhorou = True

        while melhorou:
            melhorou = False
            for i in range(1, len(rota) - 2):
                for j in range(i + 1, len(rota)):
                    if j - i == 1:
                        continue

                    nova_rota = rota[:i] + rota[i:j][::-1] + rota[j:]
                    nova_dist = self._total_dist_km(nova_rota)

                    if nova_dist < melhor_dist:
                        melhor = nova_rota
                        melhor_dist = nova_dist
                        melhorou = True

            rota = melhor

        return melhor

    def _total_dist_km(self, rota: list[dict]) -> float:
        if len(rota) < 2:
            return 0.0

        return sum(
            self._dist_km(rota[i], rota[i + 1])
            for i in range(len(rota) - 1)
        )
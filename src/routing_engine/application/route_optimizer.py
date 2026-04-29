# sales_router/src/routing_engine/application/route_optimizer.py

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
        preserve_sequence: bool = False,
        distance_service: RouteDistanceService | None = None,
    ):
        self.v_kmh = float(v_kmh)
        self.service_min = float(service_min)
        self.alpha_path = float(alpha_path)
        self.preserve_sequence = bool(preserve_sequence)

        self.distance_service = distance_service or RouteDistanceService(
            v_kmh=v_kmh,
            alpha_path=alpha_path,
        )

    # =========================================================
    # 🔧 Normalização coordenadas
    # =========================================================
    def _normalizar_coord(self, lat, lon):
        if lat is None or lon is None:
            return None, None

        try:
            lat = float(lat)
            lon = float(lon)
        except (TypeError, ValueError):
            return None, None

        # correção simples de inversão lat/lon
        if abs(lat) > abs(lon):
            return lon, lat

        return lat, lon

    # =========================================================
    # 🚀 Cálculo principal
    # =========================================================
    def calcular_rota(
        self,
        centro: dict,
        pdvs: list[dict],
        aplicar_two_opt: bool = False,
    ) -> dict:

        if not pdvs:
            return {
                "sequencia": [],
                "distancia_total_km": 0.0,
                "tempo_total_min": 0.0,
                "rota_coord": [],
            }

        centro_lat, centro_lon = self._normalizar_coord(
            centro.get("lat"),
            centro.get("lon"),
        )

        if centro_lat is None or centro_lon is None:
            logger.warning("⚠️ Centro inválido. Retornando rota vazia.")
            return {
                "sequencia": [],
                "distancia_total_km": 0.0,
                "tempo_total_min": 0.0,
                "rota_coord": [],
            }

        logger.info(
            f"🚦 Calculando rota | PDVs={len(pdvs)} | centro=({centro_lat}, {centro_lon})"
        )

        # 1) filtra PDVs válidos
        pdvs_validos = []

        for pdv in pdvs:
            lat, lon = self._normalizar_coord(pdv.get("lat"), pdv.get("lon"))
            if lat is None or lon is None:
                continue

            pdv_corrigido = pdv.copy()
            pdv_corrigido["lat"] = lat
            pdv_corrigido["lon"] = lon
            pdvs_validos.append(pdv_corrigido)

        if not pdvs_validos:
            logger.warning("⚠️ Nenhum PDV válido encontrado.")
            return {
                "sequencia": [],
                "distancia_total_km": 0.0,
                "tempo_total_min": 0.0,
                "rota_coord": [],
            }

        centro_corrigido = centro.copy()
        centro_corrigido["lat"] = centro_lat
        centro_corrigido["lon"] = centro_lon

        # 2) define sequência
        rota = self.nearest_neighbor(centro_corrigido, pdvs_validos)

        if aplicar_two_opt and len(rota) >= 4:
            rota = self.two_opt(centro_corrigido, rota)

        # 3) monta caminho centro -> rota -> centro
        pontos = [centro_corrigido] + rota + [centro_corrigido]

        coords_list = []
        for p in pontos:
            lat, lon = self._normalizar_coord(p.get("lat"), p.get("lon"))
            if lat is None or lon is None:
                continue
            coords_list.append((lat, lon))

        # 4) calcula geometria final de forma consistente
        try:
            result = self.distance_service.get_full_route(coords_list)
            if self.preserve_sequence:
                result = self.distance_service.get_full_route(
                    coords_list,
                    preserve_sequence=True,
                )
            else:
                result = self.distance_service.get_full_route(coords_list)
            rota_coords = result.get("rota_coord", [])
            total_km = float(result.get("distancia_km", 0.0))
            total_min = float(result.get("tempo_min", 0.0))
            fonte = result.get("fonte", "unknown")

        except AttributeError as e:
            logger.error(f"💥 Método inexistente em RouteDistanceService: {e}")
            raise

        except Exception as e:
            logger.warning(f"⚠️ Falha na rota completa ({e}) — fallback simples")

            rota_coords = [{"lat": lat, "lon": lon} for lat, lon in coords_list]
            total_km = self._total_dist_km(centro_corrigido, rota)
            total_min = (total_km / self.v_kmh) * 60 if self.v_kmh > 0 else 0.0
            fonte = "fallback_local"

        # 5) tempo de serviço
        total_min += len(rota) * self.service_min

        logger.success(
            f"✅ Rota concluída | PDVs={len(rota)} | dist={round(total_km, 2)} km | tempo={round(total_min, 1)} min | fonte={fonte}"
        )

        return {
            "sequencia": rota,
            "distancia_total_km": round(total_km, 2),
            "tempo_total_min": round(total_min, 1),
            "rota_coord": rota_coords,
        }

    # =========================================================
    # 📏 Distância entre pontos
    # =========================================================
    def _dist_km(self, p1, p2):

        lat1, lon1 = self._normalizar_coord(p1.get("lat"), p1.get("lon"))
        lat2, lon2 = self._normalizar_coord(p2.get("lat"), p2.get("lon"))

        if lat1 is None or lon1 is None or lat2 is None or lon2 is None:
            return 0.0

        try:
            result = self.distance_service.get_distance_time(
                (lat1, lon1),
                (lat2, lon2),
            )
            return float(result["distancia_km"])

        except Exception:
            return geodesic((lat1, lon1), (lat2, lon2)).km * self.alpha_path

    # =========================================================
    # 🔁 Nearest Neighbor
    # =========================================================
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

    # =========================================================
    # 🔁 2-opt
    # =========================================================
    def two_opt(self, centro: dict, rota: list[dict]) -> list[dict]:

        if len(rota) < 4:
            return rota

        melhor = rota[:]
        melhor_dist = self._total_dist_km(centro, melhor)
        melhorou = True

        while melhorou:
            melhorou = False

            for i in range(len(melhor) - 1):
                for j in range(i + 2, len(melhor) + 1):
                    nova_rota = melhor[:i] + melhor[i:j][::-1] + melhor[j:]
                    nova_dist = self._total_dist_km(centro, nova_rota)

                    if nova_dist < melhor_dist:
                        melhor = nova_rota
                        melhor_dist = nova_dist
                        melhorou = True

        return melhor

    # =========================================================
    # 📊 Distância total com ida e volta
    # =========================================================
    def _total_dist_km(self, centro: dict, rota: list[dict]) -> float:

        if not rota:
            return 0.0

        pontos = [centro] + rota + [centro]

        total = 0.0
        for i in range(len(pontos) - 1):
            total += self._dist_km(pontos[i], pontos[i + 1])

        return total
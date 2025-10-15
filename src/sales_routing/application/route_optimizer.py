# sales_router/src/sales_routing/application/route_optimizer.py

from geopy.distance import geodesic
from src.sales_routing.application.route_distance_service import RouteDistanceService
from loguru import logger


class RouteOptimizer:
    def __init__(self, v_kmh: float = 40.0, service_min: int = 15, alpha_path: float = 1.3):
        self.v_kmh = v_kmh
        self.service_min = service_min
        self.alpha_path = alpha_path
        self.distance_service = RouteDistanceService()

    # ============================================================
    # Normalização de coordenadas (corrige inversões lon/lat)
    # ============================================================
    def _normalizar_coord(self, lat, lon):
        """
        Corrige inversões (lon, lat) → (lat, lon) com base em magnitude esperada.
        No Brasil, |lat| < 35 e |lon| > 34.
        """
        if lat is None or lon is None:
            return None, None

        # Se latitude estiver com módulo MAIOR que longitude, inverte
        if abs(lat) > abs(lon):
            return lon, lat  # ← CORREÇÃO: inverte aqui
        return lat, lon

    # ============================================================
    # Cálculo principal de rota com tratamento de coordenadas idênticas
    # ============================================================
    def calcular_rota(self, centro: dict, pdvs: list[dict], aplicar_two_opt: bool = False) -> dict:
        if not pdvs:
            return {"sequencia": [], "distancia_total_km": 0, "tempo_total_min": 0, "rota_coord": []}

        rota = self.nearest_neighbor(centro, pdvs)
        if aplicar_two_opt:
            rota = self.two_opt(rota)

        pontos = [centro] + rota + [centro]
        rota_coords = []
        total_km, total_min = 0.0, 0.0
        warnings = 0

        for i in range(len(pontos) - 1):
            a, b = pontos[i], pontos[i + 1]
            lat_a, lon_a = self._normalizar_coord(a["lat"], a["lon"])
            lat_b, lon_b = self._normalizar_coord(b["lat"], b["lon"])

            if lat_a is None or lat_b is None:
                continue

            # ========================================================
            # NOVO 🔸 Tratamento de coordenadas idênticas
            # ========================================================
            if abs(lat_a - lat_b) < 1e-5 and abs(lon_a - lon_b) < 1e-5:
                logger.warning(
                    f"⚠️ Coordenadas idênticas detectadas entre PDVs "
                    f"({a.get('pdv_id', 'N/A')} e {b.get('pdv_id', 'N/A')}). "
                    f"Distância e tempo zerados; considerando apenas tempo de serviço."
                )
                total_min += self.service_min
                warnings += 1
                continue

            try:
                result = self.distance_service.get_distance_time((lat_a, lon_a), (lat_b, lon_b))
                total_km += result["distancia_km"]
                total_min += result["tempo_min"]
                rota_coords.extend(
                    [{"lat": c["lat"], "lon": c["lon"]} for c in result.get("rota_coord", [])]
                )
            except Exception as e:
                logger.warning(
                    f"⚠️ Fallback geodesic entre ({lat_a},{lon_a}) e ({lat_b},{lon_b}) — {e}"
                )
                total_km += geodesic((lat_a, lon_a), (lat_b, lon_b)).km * self.alpha_path
                rota_coords.append({"lat": lat_a, "lon": lon_a})
                rota_coords.append({"lat": lat_b, "lon": lon_b})

        # Tempo total inclui o tempo de serviço em cada PDV
        total_min += len(pdvs) * self.service_min

        if warnings > 0:
            logger.warning(f"⚠️ {warnings} pares consecutivos com coordenadas idênticas tratados sem erro.")

        return {
            "sequencia": rota,
            "distancia_total_km": round(total_km, 2),
            "tempo_total_min": round(total_min, 1),
            "rota_coord": rota_coords,
        }

    # ============================================================
    # Funções auxiliares
    # ============================================================
    def _dist_km(self, p1, p2):
        lat1, lon1 = self._normalizar_coord(p1["lat"], p1["lon"])
        lat2, lon2 = self._normalizar_coord(p2["lat"], p2["lon"])
        if lat1 is None or lat2 is None:
            return 0

        # Mesma lógica: se coordenadas idênticas, distância 0
        if abs(lat1 - lat2) < 1e-5 and abs(lon1 - lon2) < 1e-5:
            return 0

        try:
            result = self.distance_service.get_distance_time((lat1, lon1), (lat2, lon2))
            return result["distancia_km"]
        except Exception:
            return geodesic((lat1, lon1), (lat2, lon2)).km * self.alpha_path

    def _tempo_min(self, dist_km: float) -> float:
        return (dist_km / self.v_kmh) * 60

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
                        melhor, melhor_dist = nova_rota, nova_dist
                        melhorou = True
        return melhor

    def _total_dist_km(self, rota: list[dict]) -> float:
        if len(rota) < 2:
            return 0
        return sum(self._dist_km(rota[i], rota[i + 1]) for i in range(len(rota) - 1))

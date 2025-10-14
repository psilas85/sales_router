#sales_router/src/sales_routing/application/route_optimizer.py

from geopy.distance import geodesic
from src.sales_routing.application.route_distance_service import RouteDistanceService


class RouteOptimizer:
    def __init__(self, v_kmh: float = 40.0, service_min: int = 15, alpha_path: float = 1.3):
        self.v_kmh = v_kmh
        self.service_min = service_min
        self.alpha_path = alpha_path
        self.distance_service = RouteDistanceService()

    # ============================================================
    # Cálculo principal de rota
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

        for i in range(len(pontos) - 1):
            a, b = pontos[i], pontos[i + 1]
            try:
                result = self.distance_service.get_distance_time((a["lat"], a["lon"]), (b["lat"], b["lon"]))
                total_km += result["distancia_km"]
                total_min += result["tempo_min"]
                rota_coords.extend(result.get("rota_coord", []))
            except Exception:
                total_km += geodesic((a["lat"], a["lon"]), (b["lat"], b["lon"])).km * self.alpha_path
                rota_coords.append({"lat": a["lat"], "lon": a["lon"]})
                rota_coords.append({"lat": b["lat"], "lon": b["lon"]})

        total_min += len(pdvs) * self.service_min
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
        try:
            result = self.distance_service.get_distance_time((p1["lat"], p1["lon"]), (p2["lat"], p2["lon"]))
            return result["distancia_km"]
        except Exception:
            return geodesic((p1["lat"], p1["lon"]), (p2["lat"], p2["lon"])).km * self.alpha_path

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

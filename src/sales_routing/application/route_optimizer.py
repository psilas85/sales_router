# sales_router/src/sales_routing/application/route_optimizer.py

from geopy.distance import geodesic
import math


class RouteOptimizer:
    """
    Classe responsável por otimizar a sequência de visitas (roteirização) dentro de um subcluster.
    Implementa heurísticas leves (Nearest Neighbor + 2-Opt opcional).
    """

    def __init__(self, v_kmh: float = 40.0, service_min: int = 15, alpha_path: float = 1.3):
        self.v_kmh = v_kmh
        self.service_min = service_min
        self.alpha_path = alpha_path

    # -----------------------------
    # Distância entre dois pontos
    # -----------------------------
    def _dist_km(self, p1, p2):
        """Calcula distância geodésica entre dois PDVs (ou centro)."""
        return geodesic(
            (p1["lat"], p1["lon"]),
            (p2["lat"], p2["lon"])
        ).km * self.alpha_path

    # -----------------------------
    # Cálculo de tempo de viagem (min)
    # -----------------------------
    def _tempo_min(self, dist_km: float) -> float:
        return (dist_km / self.v_kmh) * 60

    # -----------------------------
    # Heurística Nearest Neighbor
    # -----------------------------
    def nearest_neighbor(self, centro: dict, pdvs: list[dict]) -> list[dict]:
        """Retorna sequência inicial usando vizinho mais próximo."""
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

    # -----------------------------
    # Heurística 2-Opt (opcional)
    # -----------------------------
    def two_opt(self, rota: list[dict]) -> list[dict]:
        """Otimiza rota usando o método 2-opt (reduz cruzamentos e voltas)."""
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

    # -----------------------------
    # Distância total da rota
    # -----------------------------
    def _total_dist_km(self, rota: list[dict]) -> float:
        """Distância total de uma sequência de PDVs."""
        if len(rota) < 2:
            return 0
        return sum(self._dist_km(rota[i], rota[i + 1]) for i in range(len(rota) - 1))

    # -----------------------------
    # Cálculo final de rota
    # -----------------------------
    def calcular_rota(self, centro: dict, pdvs: list[dict], aplicar_two_opt: bool = False) -> dict:
        """
        Retorna sequência otimizada de PDVs, distância total e tempo total estimado.
        Espera dicionários no formato:
        {
            "pdv_id": int,
            "lat": float,
            "lon": float
        }
        """
        if not pdvs:
            return {
                "sequencia": [],
                "distancia_total_km": 0,
                "tempo_total_min": 0,
            }

        # Passo 1 — heurística inicial (Nearest Neighbor)
        rota_inicial = self.nearest_neighbor(centro, pdvs)

        # Passo 2 — refinamento opcional
        rota_final = self.two_opt(rota_inicial) if aplicar_two_opt else rota_inicial

        # Passo 3 — cálculo final de métricas
        distancia_total = self._total_dist_km([centro] + rota_final + [centro])
        tempo_viagem = self._tempo_min(distancia_total)
        tempo_servico = len(pdvs) * self.service_min
        tempo_total = tempo_viagem + tempo_servico

        return {
            "sequencia": rota_final,
            "distancia_total_km": round(distancia_total, 2),
            "tempo_total_min": round(tempo_total, 1),
        }


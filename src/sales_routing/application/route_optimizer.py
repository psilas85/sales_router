# sales_router/src/sales_routing/application/route_optimizer.py

from geopy.distance import geodesic
from src.sales_routing.application.route_distance_service import RouteDistanceService
from loguru import logger


class RouteOptimizer:
    """
    Serviço de cálculo de rotas entre PDVs.
    Utiliza cache de distâncias, fallback geodésico e heurísticas de ordenação.
    """

    def __init__(self, v_kmh, service_min, alpha_path, distance_service: RouteDistanceService = None):
        self.v_kmh = v_kmh
        self.service_min = service_min
        self.alpha_path = alpha_path
        self.distance_service = distance_service or RouteDistanceService(v_kmh=v_kmh, alpha_path=alpha_path)

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

    def calcular_rota(self, centro: dict, pdvs: list[dict], aplicar_two_opt: bool = False) -> dict:

        if not pdvs:
            return {
                "sequencia": [],
                "distancia_total_km": 0.0,
                "tempo_total_min": 0.0,
                "rota_coord": []
            }

        logger.info(
            f"🚦 Iniciando cálculo de rota | PDVs={len(pdvs)} | Centro=({centro['lat']},{centro['lon']})"
        )

        # ============================================================
        # 1️⃣ Sequenciamento (NN + opcional 2-opt)
        # ============================================================
        rota = self.nearest_neighbor(centro, pdvs)

        if aplicar_two_opt and len(rota) >= 4:
            rota = self.two_opt(rota)

        # ============================================================
        # 2️⃣ Monta lista de pontos (com ida e volta)
        # ============================================================
        pontos = [centro] + rota + [centro]

        coords_list = []
        for p in pontos:
            lat, lon = self._normalizar_coord(p.get("lat"), p.get("lon"))
            if lat is None or lon is None:
                continue
            coords_list.append((lat, lon))

        # ============================================================
        # 3️⃣ 🚀 ROTA COMPLETA (OSRM TRIP)
        # ============================================================
        try:
            result = self.distance_service.get_full_route(coords_list)

            rota_coords = result.get("rota_coord", [])
            total_km = float(result.get("distancia_km", 0.0))
            total_min = float(result.get("tempo_min", 0.0))
            fonte = result.get("fonte", "unknown")

        except Exception as e:
            logger.warning(f"⚠️ Falha rota completa ({e}) — fallback segmentado")

            # ========================================================
            # 🔁 FALLBACK SEGMENTADO (robustez)
            # ========================================================
            rota_coords = []
            total_km = 0.0
            total_min = 0.0

            for i in range(len(coords_list) - 1):
                a = coords_list[i]
                b = coords_list[i + 1]

                try:
                    res = self.distance_service.get_distance_time(a, b)

                    total_km += float(res["distancia_km"])
                    total_min += float(res["tempo_min"])

                    seg = res.get("rota_coord") or [
                        {"lat": a[0], "lon": a[1]},
                        {"lat": b[0], "lon": b[1]},
                    ]

                    # evita duplicar pontos
                    if rota_coords and seg:
                        if (
                            rota_coords[-1]["lat"] == seg[0]["lat"]
                            and rota_coords[-1]["lon"] == seg[0]["lon"]
                        ):
                            seg = seg[1:]

                    rota_coords.extend(seg)

                except Exception:
                    continue

            fonte = "fallback_segmentado"

        # ============================================================
        # 4️⃣ Garantir início e fim no centro
        # ============================================================
        if rota_coords:
            first = rota_coords[0]
            last = rota_coords[-1]

            if abs(first["lat"] - centro["lat"]) > 1e-4:
                rota_coords.insert(0, {"lat": centro["lat"], "lon": centro["lon"]})

            if abs(last["lat"] - centro["lat"]) > 1e-4:
                rota_coords.append({"lat": centro["lat"], "lon": centro["lon"]})
        else:
            # fallback extremo
            rota_coords = (
                [{"lat": centro["lat"], "lon": centro["lon"]}]
                + [{"lat": p["lat"], "lon": p["lon"]} for p in rota]
                + [{"lat": centro["lat"], "lon": centro["lon"]}]
            )

        # ============================================================
        # 5️⃣ Tempo de serviço (paradas)
        # ============================================================
        total_min += len(rota) * self.service_min

        # ============================================================
        # 5b) Parciais — total sem a perna final (último PDV → centro).
        # Representa "até concluir o último atendimento" — métrica
        # operacional útil pra dimensionar SLA do consultor sem contar
        # o retorno ao depot.
        # ============================================================
        dist_parcial_km = total_km
        tempo_parcial_min = total_min
        if len(coords_list) >= 2:
            try:
                last_a = coords_list[-2]
                last_b = coords_list[-1]
                last_leg = self.distance_service.get_distance_time(last_a, last_b)
                last_km = float(last_leg.get("distancia_km", 0.0))
                last_min = float(last_leg.get("tempo_min", 0.0))
                dist_parcial_km = max(0.0, total_km - last_km)
                tempo_parcial_min = max(0.0, total_min - last_min)
            except Exception as e:
                logger.warning(
                    f"⚠️ Falha ao computar parcial (último trecho): {e}"
                )

        # ============================================================
        # 6️⃣ LOG FINAL
        # ============================================================
        logger.success(
            f"✅ Rota concluída | PDVs={len(rota)} | dist={round(total_km,2)} km | "
            f"tempo={round(total_min,1)} min | fonte={fonte}"
        )

        return {
            "sequencia": rota,
            "distancia_total_km": round(total_km, 2),
            "tempo_total_min": round(total_min, 1),
            "dist_parcial_km": round(dist_parcial_km, 2),
            "tempo_parcial_min": round(tempo_parcial_min, 1),
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

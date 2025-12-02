#sales_router/src/sales_clusterization/mkp_pdv/domain/pdv_assignment.py

# sales_router/src/sales_clusterization/mkp_pdv/domain/pdv_assignment.py

from .haversine_utils import haversine
import numpy as np

class PDVAssignment:

    @staticmethod
    def atribuir(pdvs, centros):
        """
        Atribui cada PDV ao centro mais próximo usando haversine.
        PDVs = lista de dicts
        Centros = lista de dicts
        """

        centros_lat = np.array([c["lat"] for c in centros])
        centros_lon = np.array([c["lon"] for c in centros])

        resultados = []

        for p in pdvs:
            # distâncias para todos os centros
            dists = [
                haversine(p["lat"], p["lon"], clat, clon)
                for clat, clon in zip(centros_lat, centros_lon)
            ]

            idx = int(np.argmin(dists))
            centro = centros[idx]

            resultados.append({
                **p,
                "cluster_id": idx + 1,
                "cluster_lat": centro["lat"],
                "cluster_lon": centro["lon"],
                "cluster_bairro": centro.get("bairro", None),
                "dist_km": dists[idx],
                "tempo_min": dists[idx] / 0.4,  # simples conversão
            })

        return resultados

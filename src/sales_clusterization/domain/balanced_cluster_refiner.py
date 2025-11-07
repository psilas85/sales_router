# ============================================================
# 📦 src/sales_clusterization/domain/balanced_cluster_refiner.py
# ============================================================

import numpy as np
from loguru import logger
from math import radians, sin, cos, sqrt, atan2
from typing import List, Tuple, Dict


# ============================================================
# 🧮 Funções auxiliares
# ============================================================
def _haversine_km(lat1, lon1, lat2, lon2):
    """Calcula a distância Haversine em km."""
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
    return R * 2 * atan2(sqrt(a), sqrt(1 - a))


def _calcular_centro(coords: np.ndarray) -> Tuple[float, float]:
    """Calcula centro geométrico simples."""
    return float(coords[:, 0].mean()), float(coords[:, 1].mean())


def _centro_ponderado_por_densidade(coords: np.ndarray, vizinhos=5) -> Tuple[float, float]:
    """
    Ajusta o centro geométrico deslocando-o para a região mais densa de PDVs.
    Usa média ponderada pela densidade local.
    """
    from sklearn.neighbors import NearestNeighbors

    if len(coords) < 3:
        return _calcular_centro(coords)

    k = min(vizinhos, len(coords) - 1)
    nn = NearestNeighbors(n_neighbors=k)
    nn.fit(coords)
    dist, _ = nn.kneighbors(coords)
    densidade = 1 / (dist.mean(axis=1) + 1e-6)
    densidade /= densidade.sum()
    centro_lat = np.sum(coords[:, 0] * densidade)
    centro_lon = np.sum(coords[:, 1] * densidade)
    return float(centro_lat), float(centro_lon)


# ============================================================
# ⚙️ Classe principal
# ============================================================
class BalancedClusterRefiner:
    """
    Rebalanceia clusters de PDVs para respeitar limites máximos de PDVs por cluster.
    - Redistribui PDVs excedentes do cluster mais cheio para o mais próximo com espaço.
    - Mantém o centro de cada cluster atualizado dinamicamente.
    - Recalcula centro ponderado após cada redistribuição.
    """

    def __init__(self, max_pdv_cluster: int, tolerancia: int = 2):
        self.max_pdv_cluster = max_pdv_cluster
        self.tolerancia = tolerancia

    # --------------------------------------------------------
    def refinar(self, coords: np.ndarray, labels: np.ndarray) -> np.ndarray:
        """
        Recebe coordenadas (lat, lon) e labels do KMeans.
        Retorna novos labels balanceados mantendo coerência espacial.
        """
        labels = np.array(labels, dtype=int)
        n_clusters = len(np.unique(labels))
        logger.debug(f"🔄 Rebalanceando {n_clusters} clusters | limite={self.max_pdv_cluster}")

        # Cria dicionário de clusters
        clusters = {
            c: np.where(labels == c)[0].tolist()
            for c in np.unique(labels)
        }

        # Calcula centros iniciais
        centros = {
            c: _centro_ponderado_por_densidade(coords[clusters[c]])
            for c in clusters
        }

        # Redistribuição iterativa
        alterado = True
        iteracao = 0
        while alterado and iteracao < 10:
            iteracao += 1
            alterado = False

            tamanhos = {c: len(v) for c, v in clusters.items()}
            cheio = [c for c, t in tamanhos.items() if t > self.max_pdv_cluster + self.tolerancia]
            vazio = [c for c, t in tamanhos.items() if t < self.max_pdv_cluster - self.tolerancia]

            if not cheio or not vazio:
                break

            for c_cheio in cheio:
                while len(clusters[c_cheio]) > self.max_pdv_cluster and vazio:
                    # Cluster receptor mais próximo
                    distancias = {
                        c_vazio: _haversine_km(
                            centros[c_cheio][0], centros[c_cheio][1],
                            centros[c_vazio][0], centros[c_vazio][1]
                        )
                        for c_vazio in vazio
                    }
                    c_destino = min(distancias, key=distancias.get)

                    # Move PDV mais próximo do destino
                    pdvs_cheio = clusters[c_cheio]
                    dist_pdv_destino = [
                        _haversine_km(coords[i][0], coords[i][1], centros[c_destino][0], centros[c_destino][1])
                        for i in pdvs_cheio
                    ]
                    idx_pdv = pdvs_cheio[int(np.argmin(dist_pdv_destino))]
                    clusters[c_cheio].remove(idx_pdv)
                    clusters[c_destino].append(idx_pdv)
                    labels[idx_pdv] = c_destino
                    alterado = True

                    # Atualiza centros e tamanhos
                    centros[c_cheio] = _centro_ponderado_por_densidade(coords[clusters[c_cheio]])
                    centros[c_destino] = _centro_ponderado_por_densidade(coords[clusters[c_destino]])
                    tamanhos[c_cheio] = len(clusters[c_cheio])
                    tamanhos[c_destino] = len(clusters[c_destino])

                    # Atualiza listas de estado
                    if tamanhos[c_destino] >= self.max_pdv_cluster - self.tolerancia:
                        vazio.remove(c_destino)
                    if tamanhos[c_cheio] <= self.max_pdv_cluster + self.tolerancia:
                        break

            logger.debug(f"🌀 Iteração {iteracao}: tamanhos → {tamanhos}")

        logger.success(f"⚖️ Rebalanceamento concluído em {iteracao} iterações.")
        return labels

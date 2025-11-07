# ============================================================
# 📦 src/sales_clusterization/domain/capacitated_kmeans.py
# ============================================================

import numpy as np
from sklearn.cluster import KMeans
from sklearn.neighbors import NearestNeighbors
from loguru import logger
from math import radians, sin, cos, sqrt, atan2


# ============================================================
# 📍 Funções auxiliares
# ============================================================

def _haversine_km(lat1, lon1, lat2, lon2):
    """Calcula a distância Haversine entre dois pontos (em km)."""
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


def _redistribuir_capacidade(coords, labels, centers, max_capacity, tolerancia=2, max_iter=10):
    """
    Redistribui pontos entre clusters para respeitar a capacidade máxima.
    Heurística simples baseada em distância aos centros.
    """
    labels = np.array(labels, dtype=int)
    for it in range(max_iter):
        unique, counts = np.unique(labels, return_counts=True)
        excesso = {u: c - max_capacity for u, c in zip(unique, counts) if c > max_capacity + tolerancia}
        faltando = {u: max_capacity - c for u, c in zip(unique, counts) if c < max_capacity - tolerancia}

        logger.debug(f"🔁 Iteração {it+1}: excesso={excesso} | faltando={faltando}")
        if not excesso and not faltando:
            break

        for cid_excesso, qtd_excesso in excesso.items():
            idx_excesso = np.where(labels == cid_excesso)[0]
            if len(idx_excesso) == 0:
                continue

            dist_to_centers = np.linalg.norm(coords[idx_excesso][:, None, :] - centers, axis=2)
            ordenados = np.argsort(-dist_to_centers[:, cid_excesso])  # mais distantes primeiro

            for i in ordenados[:qtd_excesso]:
                destinos_validos = [d for d, f in faltando.items() if f > 0]
                if not destinos_validos:
                    break
                dist_ao_destino = {d: dist_to_centers[i, d] for d in destinos_validos}
                destino = min(dist_ao_destino, key=dist_ao_destino.get)
                labels[idx_excesso[i]] = destino
                faltando[destino] -= 1

        # Atualiza centros após redistribuição
        for cid in np.unique(labels):
            cluster_pts = coords[labels == cid]
            if len(cluster_pts):
                centers[cid] = cluster_pts.mean(axis=0)

    logger.success(f"⚖️ Redistribuição de capacidade concluída após {it+1} iterações.")
    return labels, centers


# ============================================================
# 🧭 Função de refinamento espacial (remoção de "ilhas")
# ============================================================

def refinamento_espacial_ilhas(pdvs, labels, centers, max_pdv_cluster, raio_vizinhanca_km=1.0):
    """
    Detecta e corrige 'ilhas' — PDVs isolados do seu cluster.
    Reatribui PDVs ao cluster mais próximo se:
      - Possuem menos de 2 vizinhos do mesmo cluster num raio de 1 km;
      - Estão mais próximos do centróide de outro cluster.
    """
    if len(pdvs) < 5:
        return labels, centers

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    coords_rad = np.radians(coords)
    n = len(coords)
    k_clusters = len(centers)
    R = 6371.0

    # 🔍 Calcula vizinhos geográficos
    nn = NearestNeighbors(radius=raio_vizinhanca_km / R, metric="haversine")
    nn.fit(coords_rad)
    vizinhos = nn.radius_neighbors(coords_rad, return_distance=False)

    labels_corrigidos = np.array(labels, dtype=int)
    alterados = 0

    for i in range(n):
        cluster_atual = labels_corrigidos[i]
        viz = vizinhos[i]
        if len(viz) < 2:
            mesmos = sum(labels_corrigidos[j] == cluster_atual for j in viz)
            if mesmos < 2:
                dist_cents = np.array([
                    _haversine_km(pdvs[i].lat, pdvs[i].lon, c[0], c[1])
                    for c in centers
                ])
                cluster_novo = int(np.argmin(dist_cents))
                if cluster_novo != cluster_atual:
                    labels_corrigidos[i] = cluster_novo
                    alterados += 1

    # ⚖️ Ajuste fino de capacidade
    unique, counts = np.unique(labels_corrigidos, return_counts=True)
    excesso = {u: c - max_pdv_cluster for u, c in zip(unique, counts) if c > max_pdv_cluster}
    faltando = {u: max_pdv_cluster - c for u, c in zip(unique, counts) if c < max_pdv_cluster - 2}

    for cid_excesso, qtd_excesso in excesso.items():
        idx_excesso = np.where(labels_corrigidos == cid_excesso)[0]
        for i in idx_excesso[:qtd_excesso]:
            if not faltando:
                break
            destinos_validos = list(faltando.keys())
            dist_cents = np.array([
                _haversine_km(pdvs[i].lat, pdvs[i].lon, centers[d][0], centers[d][1])
                for d in destinos_validos
            ])
            destino = destinos_validos[int(np.argmin(dist_cents))]
            labels_corrigidos[i] = destino
            faltando[destino] -= 1
            if faltando[destino] <= 0:
                del faltando[destino]

    logger.info(f"🧭 Refinamento espacial: {alterados} PDVs reatribuídos para eliminar 'ilhas'.")
    return labels_corrigidos, centers


# ============================================================
# 🚀 Algoritmo principal
# ============================================================

def capacitated_kmeans(pdvs, max_capacity: int = 200, random_state: int = 42):
    """
    Executa uma variação simplificada do Capacitated K-Means,
    garantindo que cada cluster tenha no máximo `max_capacity` PDVs.
    Inclui redistribuição de capacidade e refinamento espacial.
    """
    logger.info(f"🚀 Iniciando Capacitated K-Means | capacidade máxima={max_capacity}")

    # 1️⃣ Extrai coordenadas
    coords = np.array([[p.lat, p.lon] for p in pdvs])
    n_pdvs = len(coords)
    k = max(1, round(n_pdvs / max_capacity))
    logger.info(f"📊 Total de {n_pdvs} PDVs → K inicial ≈ {k}")

    # 2️⃣ KMeans inicial
    kmeans = KMeans(n_clusters=k, n_init=20, random_state=random_state)
    labels = kmeans.fit_predict(coords)
    centers = kmeans.cluster_centers_

    # 3️⃣ Redistribuição para respeitar a capacidade
    labels, centers = _redistribuir_capacidade(coords, labels, centers, max_capacity)

    # 4️⃣ Pós-processamento: correção de 'ilhas'
    labels, centers = refinamento_espacial_ilhas(pdvs, labels, centers, max_capacity)

    # 5️⃣ Diagnóstico final
    unique, counts = np.unique(labels, return_counts=True)
    media_cluster = np.mean(counts)
    desvio_cluster = np.std(counts)
    max_diff = max(counts) - min(counts)

    # 6️⃣ Distância média intra-cluster (coerência geográfica)
    dist_medias = []
    for cid in np.unique(labels):
        cluster_points = [p for p, lbl in zip(pdvs, labels) if lbl == cid]
        if not cluster_points:
            continue
        centro = centers[cid]
        dists = [_haversine_km(p.lat, p.lon, centro[0], centro[1]) for p in cluster_points]
        dist_medias.append(np.mean(dists))
    dist_media_geral = np.mean(dist_medias) if dist_medias else 0

    logger.info(
        f"📈 Diagnóstico final: média={media_cluster:.1f} | desvio={desvio_cluster:.1f} | "
        f"max_diff={max_diff} | dist_média_intra={dist_media_geral:.2f} km | tamanhos={dict(zip(unique, counts))}"
    )

    return labels, centers

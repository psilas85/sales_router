# ============================================================
# 📦 src/sales_clusterization/domain/radial_geo_continuous.py
# ============================================================

import numpy as np
from sklearn.neighbors import NearestNeighbors
from loguru import logger
from math import radians, sin, cos, sqrt, atan2


# ============================================================
# 🔹 Distância Haversine
# ============================================================
def _haversine_km(lat1, lon1, lat2, lon2):
    """Calcula a distância Haversine em km."""
    R = 6371.0
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2) ** 2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2) ** 2
    return 2 * R * atan2(sqrt(a), sqrt(1 - a))


# ============================================================
# 🔹 Seleção inicial de centros distantes (Haversine)
# ============================================================
def _farthest_point_sampling(coords, k):
    """Seleciona k pontos distantes entre si como centros iniciais (Haversine)."""
    n = len(coords)
    centers = [np.random.randint(0, n)]
    for _ in range(1, k):
        dists = np.array([
            np.min([
                _haversine_km(coords[i][0], coords[i][1], coords[c][0], coords[c][1])
                for c in centers
            ])
            for i in range(n)
        ])
        centers.append(np.argmax(dists))
    return coords[centers]


# ============================================================
# 🌐 Clusterização Radial Geo Contínua (versão otimizada)
# ============================================================
def radial_geo_continuous(pdvs, k, max_pdv_cluster, tolerancia=3, max_iter=15):
    """
    Clusterização radial contínua:
    - Expande clusters radialmente com raio adaptativo e limite de iterações.
    - Mantém continuidade espacial e equilíbrio.
    - Garante 100% dos PDVs atribuídos.
    """

    logger.info(f"🌀 Iniciando clusterização contínua: {len(pdvs)} PDVs | K={k} | max={max_pdv_cluster}/cluster")

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    n_pdvs = len(coords)

    # Centros iniciais via farthest-point sampling
    centers = _farthest_point_sampling(coords, k)
    logger.debug(f"🎯 Centros iniciais selecionados: {centers}")

    labels = np.full(n_pdvs, -1, dtype=int)
    assigned = np.zeros(n_pdvs, dtype=bool)
    nn_global = NearestNeighbors(n_neighbors=min(50, n_pdvs), metric="haversine").fit(np.radians(coords))

    # ============================================================
    # 1️⃣ Expansão radial adaptativa (com limite de tentativas)
    # ============================================================
    for cid, center in enumerate(centers):
        center_idx = np.argmin([
            _haversine_km(center[0], center[1], coords[i][0], coords[i][1]) for i in range(n_pdvs)
        ])
        labels[center_idx] = cid
        assigned[center_idx] = True
        frontier = [center_idx]
        raio_km = 1.0
        tentativas = 0

        while np.sum(labels == cid) < max_pdv_cluster and tentativas < 30:
            novos_vizinhos = []
            for idx in frontier:
                dists, neigh_idx = nn_global.radius_neighbors(
                    [np.radians(coords[idx])],
                    radius=raio_km / 6371.0  # km → radianos
                )
                for n_idx in neigh_idx[0]:
                    if not assigned[n_idx]:
                        novos_vizinhos.append(n_idx)

            if not novos_vizinhos:
                raio_km *= 1.5
                tentativas += 1
                if raio_km > 20:
                    logger.warning(
                        f"⏳ Cluster {cid}: limite de raio atingido ({raio_km:.1f} km) — "
                        f"{np.sum(labels == cid)} PDVs atribuídos."
                    )
                    break
                continue

            for n_idx in novos_vizinhos:
                if np.sum(labels == cid) >= max_pdv_cluster:
                    break
                labels[n_idx] = cid
                assigned[n_idx] = True
                frontier.append(n_idx)

            tentativas += 1

        logger.debug(
            f"🌍 Cluster {cid}: {np.sum(labels == cid)} PDVs atribuídos "
            f"(fase inicial, {tentativas} iterações, raio={raio_km:.1f} km)"
        )

    # ============================================================
    # 2️⃣ Rebalanceamento ponderado
    # ============================================================
    for iteration in range(max_iter):
        unique, counts = np.unique(labels, return_counts=True)
        excesso = {u: c - max_pdv_cluster for u, c in zip(unique, counts) if c > max_pdv_cluster + tolerancia}
        faltando = {u: max_pdv_cluster - c for u, c in zip(unique, counts) if c < max_pdv_cluster - tolerancia}

        logger.debug(f"🔁 Iteração {iteration + 1}: excesso={excesso} | faltando={faltando}")
        if not excesso and not faltando:
            logger.success(f"✅ Convergência alcançada após {iteration + 1} iterações.")
            break

        for cid_excesso, qtd_excesso in excesso.items():
            idx_cluster = np.where(labels == cid_excesso)[0]
            if len(idx_cluster) == 0:
                continue

            centro_atual = centers[cid_excesso]
            dist = np.array([
                _haversine_km(centro_atual[0], centro_atual[1], coords[i][0], coords[i][1])
                for i in idx_cluster
            ])
            ordem = np.argsort(-dist)
            candidatos = idx_cluster[ordem[:qtd_excesso * 2]]

            for i in candidatos:
                destinos_validos = [d for d, f in faltando.items() if f > 0]
                if not destinos_validos:
                    break

                dist_to_centers = [
                    _haversine_km(coords[i][0], coords[i][1], centers[d][0], centers[d][1])
                    for d in destinos_validos
                ]

                # Heurística de custo (equilíbrio / distância²)
                score = [(d, faltando[d] / (dist_to_centers[j] ** 2 + 1e-6)) for j, d in enumerate(destinos_validos)]
                destino = max(score, key=lambda x: x[1])[0]

                labels[i] = destino
                faltando[destino] -= 1
                excesso[cid_excesso] -= 1

        # Atualiza centros
        for cid in np.unique(labels):
            pts = coords[labels == cid]
            if len(pts) > 0:
                centers[cid] = pts.mean(axis=0)

    # ============================================================
    # 3️⃣ Atribui PDVs não classificados (-1) ao cluster mais próximo
    # ============================================================
    unassigned_idx = np.where(labels == -1)[0]
    if len(unassigned_idx) > 0:
        logger.warning(f"⚠️ {len(unassigned_idx)} PDVs ficaram não atribuídos após expansão.")
        assigned_coords = coords[labels != -1]
        assigned_labels = labels[labels != -1]
        nbrs = NearestNeighbors(n_neighbors=1, metric="haversine").fit(np.radians(assigned_coords))
        _, idxs = nbrs.kneighbors(np.radians(coords[unassigned_idx]))
        for i, idx in zip(unassigned_idx, idxs[:, 0]):
            labels[i] = assigned_labels[idx]
        logger.success(f"✅ {len(unassigned_idx)} PDVs não atribuídos foram realocados com sucesso.")

    # ============================================================
    # 4️⃣ Suavização de fronteiras (iterativa até estabilidade)
    # ============================================================
    nbrs = NearestNeighbors(n_neighbors=6, metric="haversine").fit(np.radians(coords))
    _, indices_vizinhos = nbrs.kneighbors(np.radians(coords))

    for iter_smooth in range(5):
        alterados = 0
        for i in range(n_pdvs):
            vizinhos_labels = labels[indices_vizinhos[i][1:]]
            if len(vizinhos_labels[vizinhos_labels >= 0]) == 0:
                continue
            mais_frequente = np.bincount(vizinhos_labels[vizinhos_labels >= 0]).argmax()
            if np.sum(vizinhos_labels == mais_frequente) >= 3 and labels[i] != mais_frequente:
                labels[i] = mais_frequente
                alterados += 1
        if alterados == 0:
            break
    logger.info(f"🔄 Suavização concluída após {iter_smooth + 1} iterações.")

    # ============================================================
    # 5️⃣ Diagnóstico final e garantia de completude
    # ============================================================
    if np.any(labels == -1):
        logger.warning(f"🚨 {np.sum(labels == -1)} PDVs permanecem sem cluster — forçando atribuição final.")
        unassigned_idx = np.where(labels == -1)[0]
        assigned_coords = coords[labels != -1]
        assigned_labels = labels[labels != -1]
        nbrs = NearestNeighbors(n_neighbors=1, metric="haversine").fit(np.radians(assigned_coords))
        _, idxs = nbrs.kneighbors(np.radians(coords[unassigned_idx]))
        for i, idx in zip(unassigned_idx, idxs[:, 0]):
            labels[i] = assigned_labels[idx]
        logger.success("✅ 100% dos PDVs atribuídos a clusters.")

    distrib = {int(c): int(np.sum(labels == c)) for c in np.unique(labels)}
    raios = [
        np.median([
            _haversine_km(c[0], c[1], x[0], x[1])
            for x in coords[labels == i]
        ]) if np.sum(labels == i) > 0 else 0
        for i, c in enumerate(centers)
    ]

    logger.info(f"📊 Distribuição final: {distrib}")
    logger.info(f"📏 Raio mediano por cluster (km): {np.round(raios, 2).tolist()}")
    logger.success("🌐 Clusterização radial contínua concluída com sucesso.")

    return labels, centers

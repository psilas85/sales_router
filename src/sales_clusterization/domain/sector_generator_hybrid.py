# src/sales_clusterization/domain/sector_generator_hybrid.py

import math
import time
import numpy as np
from sklearn.cluster import KMeans
from loguru import logger
from typing import List, Tuple
from src.sales_clusterization.domain.entities import PDV, Setor
from src.sales_clusterization.domain.k_estimator import _haversine_km
from src.sales_clusterization.domain.sector_generator import dbscan_setores, _raios_cluster


def dbscan_kmeans_balanceado(
    pdvs: List[PDV],
    max_pdv_cluster: int = 300,
    eps_km: float = None,
    min_samples: int = None,
) -> Tuple[List[Setor], np.ndarray]:
    """
    Pipeline híbrido de clusterização:
    1️⃣ Executa DBSCAN adaptativo (com reatribuição de ruídos)
    2️⃣ Aplica KMeans corretivo nos clusters com mais de max_pdv_cluster PDVs
    3️⃣ Garante que 100% dos PDVs sejam incluídos em clusters finais
    """

    if not pdvs:
        logger.warning("⚠️ Nenhum PDV recebido para clusterização híbrida.")
        return [], []

    inicio_total = time.time()
    logger.info(f"🚀 Iniciando DBSCAN híbrido | total PDVs={len(pdvs)} | limite={max_pdv_cluster} PDVs/cluster")

    # ------------------------------------------------------
    # 1️⃣ DBSCAN inicial (já reatribui ruídos automaticamente)
    # ------------------------------------------------------
    setores, labels = dbscan_setores(pdvs, eps_km=eps_km, min_samples=min_samples)
    coords = np.array([[p.lat, p.lon] for p in pdvs], dtype=np.float64)
    labels = np.array(labels)

    n_clusters_iniciais = len(set(labels))
    ruido_inicial = np.sum(labels == -1)
    logger.info(f"📊 DBSCAN inicial: {n_clusters_iniciais} clusters | {ruido_inicial} PDVs de ruído (reatrib.)")

    # ------------------------------------------------------
    # 2️⃣ KMeans balanceador — subdivide clusters grandes
    # ------------------------------------------------------
    novos_labels = labels.copy()
    cluster_id_offset = int(max(labels)) + 1 if len(labels) > 0 else 0
    subdivisoes_realizadas = 0
    total_pdv_redistribuido = 0

    for s in setores:
        if s.n_pdvs > max_pdv_cluster:
            idx_cluster = np.where(labels == s.cluster_label)[0]
            subset_coords = coords[idx_cluster]
            n_pdv = len(subset_coords)
            n_split = math.ceil(n_pdv / max_pdv_cluster)

            logger.warning(
                f"⚖️ Cluster {s.cluster_label} excede {max_pdv_cluster} PDVs "
                f"({n_pdv} PDVs) → aplicando KMeans (n_clusters={n_split})"
            )

            km = KMeans(n_clusters=n_split, random_state=42, n_init="auto")
            sub_labels = km.fit_predict(subset_coords)

            for i, local_label in zip(idx_cluster, sub_labels):
                novos_labels[i] = cluster_id_offset + local_label

            cluster_id_offset += n_split
            subdivisoes_realizadas += 1
            total_pdv_redistribuido += n_pdv

    # ------------------------------------------------------
    # 3️⃣ Recalcula centros e raios
    # ------------------------------------------------------
    setores_final: List[Setor] = []
    for cid in sorted(set(novos_labels)):
        idx = np.where(novos_labels == cid)[0]
        pts = [coords[i] for i in idx]
        if not pts:
            continue
        c = (float(np.mean([p[0] for p in pts])), float(np.mean([p[1] for p in pts])))
        med, p95 = _raios_cluster(c, pts)
        setores_final.append(
            Setor(
                cluster_label=int(cid),
                centro_lat=c[0],
                centro_lon=c[1],
                n_pdvs=len(pts),
                raio_med_km=float(med),
                raio_p95_km=float(p95),
            )
        )

    # ------------------------------------------------------
    # 4️⃣ Logs consolidados de diagnóstico
    # ------------------------------------------------------
    tempo_total = round(time.time() - inicio_total, 2)
    logger.info("📊 Resumo do balanceamento híbrido:")
    logger.info(f"   - Clusters iniciais (DBSCAN): {n_clusters_iniciais}")
    logger.info(f"   - Clusters finais (após balanceamento): {len(setores_final)}")
    logger.info(f"   - Clusters subdivididos: {subdivisoes_realizadas}")
    logger.info(f"   - PDVs redistribuídos via KMeans: {total_pdv_redistribuido}")
    logger.info(f"   - Tempo total de execução: {tempo_total}s")

    logger.success(
        f"✅ Híbrido concluído: {len(setores_final)} clusters finais "
        f"({max_pdv_cluster} PDVs/cluster máx.) | Duração total {tempo_total}s"
    )

    return setores_final, novos_labels

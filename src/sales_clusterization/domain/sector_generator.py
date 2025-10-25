# src/sales_clusterization/domain/sector_generator.py

from typing import List, Tuple
import numpy as np
import time
import psutil
from joblib import parallel_backend
from sklearn.cluster import KMeans, DBSCAN
from loguru import logger
from .entities import PDV, Setor
from .k_estimator import _haversine_km
import math


# ==========================================================
# 🔹 Funções auxiliares
# ==========================================================
def _raios_cluster(centro: Tuple[float, float], pts: List[Tuple[float, float]]):
    if not pts:
        return 0.0, 0.0
    dists = [_haversine_km(centro, p) for p in pts]
    dists.sort()
    med = dists[len(dists) // 2]
    p95 = dists[int(0.95 * len(dists)) - 1] if len(dists) >= 2 else dists[-1]
    return med, p95


# ==========================================================
# 🧠 KMEANS com paralelismo interno (seguro)
# ==========================================================
def kmeans_setores(pdvs: List[PDV], k: int, random_state: int = 42):
    """
    Executa KMeans com paralelismo interno via joblib.
    - Remove PDVs com coordenadas NaN ou inválidas.
    - Loga número de CPUs disponíveis e tempo total.
    """
    if not pdvs:
        return [], []

    start = time.time()

    # ✅ Limpa NaNs e coordenadas inválidas
    coords = [(p.lat, p.lon) for p in pdvs if p.lat and p.lon and not np.isnan(p.lat) and not np.isnan(p.lon)]
    if len(coords) < 2:
        logger.warning(f"⚠️ Dados insuficientes para KMeans ({len(coords)} pontos válidos).")
        return [], []

    X = np.array(coords, dtype=np.float64)
    cpu_cores = psutil.cpu_count(logical=True)

    km = KMeans(
        n_clusters=k,
        random_state=random_state,
        n_init="auto",
        algorithm="lloyd"
    )

    logger.info(f"🧮 Executando KMeans com {cpu_cores} núcleos (loky backend)...")

    with parallel_backend("loky", n_jobs=-1):
        labels = km.fit_predict(X)

    centers = km.cluster_centers_

    setores: List[Setor] = []
    for cid in range(k):
        idx = np.where(labels == cid)[0]
        pts = [coords[i] for i in idx]
        c = tuple(centers[cid])
        med, p95 = _raios_cluster(c, pts)
        setores.append(
            Setor(
                cluster_label=int(cid),
                centro_lat=float(c[0]),
                centro_lon=float(c[1]),
                n_pdvs=int(len(idx)),
                raio_med_km=float(med),
                raio_p95_km=float(p95),
            )
        )

    elapsed = round(time.time() - start, 2)
    logger.info(f"✅ KMeans concluído (K={k}, {len(coords)} pontos, {cpu_cores} CPUs, {elapsed}s).")
    return setores, labels


# ==========================================================
# 🧠 DBSCAN adaptativo com reatribuição de ruídos
# ==========================================================
def dbscan_setores(
    pdvs: List[PDV],
    eps_km: float = None,
    min_samples: int = None,
    eps_min_km: float = 2.0,
    eps_max_km: float = 6.0,
    min_samples_min: int = 10,
    min_samples_max: int = 30,
):
    """
    Executa DBSCAN com ajuste automático de eps_km e min_samples com base na densidade média de PDVs.
    - Calcula espaçamento médio entre PDVs (distância mediana ao vizinho mais próximo)
    - Define eps_km e min_samples dinamicamente conforme densidade
    - Reatribui PDVs de ruído ao cluster mais próximo
    - Garante cobertura 100%
    """

    if not pdvs:
        return [], []

    start = time.time()
    coords = np.array([[p.lat, p.lon] for p in pdvs if p.lat and p.lon], dtype=np.float64)
    n = len(coords)
    if n < 2:
        logger.warning("⚠️ Dados insuficientes para DBSCAN.")
        return [], []

    # =======================================================
    # 🔍 Cálculo de densidade média (espaçamento entre PDVs)
    # =======================================================
    sample = coords[np.random.choice(n, min(1000, n), replace=False)]
    dists = []
    for s in sample:
        diff = coords - s
        dlat = np.radians(diff[:, 0])
        dlon = np.radians(diff[:, 1])
        a = np.sin(dlat / 2) ** 2 + np.cos(np.radians(s[0])) * np.cos(np.radians(coords[:, 0])) * np.sin(dlon / 2) ** 2
        dist = 2 * 6371 * np.arcsin(np.sqrt(a))
        dists.append(np.min(dist[dist > 0]))
    med_dist_km = np.median(dists)

    # =======================================================
    # ⚙️ Ajuste automático de eps_km e min_samples
    # =======================================================
    if eps_km is None:
        eps_km = np.clip(med_dist_km * 20, eps_min_km, eps_max_km)

    if min_samples is None:
        # Densidades maiores => min_samples maior (mais rigoroso)
        densidade_rel = 1 / (med_dist_km + 1e-6)
        densidade_norm = np.clip((densidade_rel - 0.05) / 0.1, 0, 1)
        min_samples = int(min_samples_min + densidade_norm * (min_samples_max - min_samples_min))
        min_samples = int(np.clip(min_samples, min_samples_min, min_samples_max))

    logger.info(f"📏 eps_km ajustado: {eps_km:.2f} km | min_samples ajustado: {min_samples} | densidade média: {med_dist_km:.2f} km")

    eps_deg = eps_km / 111.0  # conversão km → graus

    # =======================================================
    # 🚀 Execução do DBSCAN
    # =======================================================
    db = DBSCAN(eps=eps_deg, min_samples=min_samples)
    labels = db.fit_predict(coords)

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    ruido_count = np.sum(labels == -1)
    logger.info(f"📊 DBSCAN terminou em {round(time.time() - start, 2)}s com {n_clusters} clusters e {ruido_count} PDVs de ruído.")

    # =======================================================
    # 🔁 Reatribuição de ruídos (com tratamento para clusters vazios)
    # =======================================================
    if ruido_count > 0:
        ruido_idx = np.where(labels == -1)[0]
        cluster_centers = []
        for cid in sorted(set(labels)):
            if cid == -1:
                continue
            pts = coords[labels == cid]
            if len(pts) == 0:
                continue
            c = (float(np.mean(pts[:, 0])), float(np.mean(pts[:, 1])))
            cluster_centers.append((cid, c))

        if not cluster_centers:
            # Nenhum cluster formado → cria um cluster único com todos os PDVs
            labels[:] = 0
            logger.warning("⚠️ Nenhum cluster DBSCAN formado — todos os PDVs agrupados em um único cluster.")
        else:
            reassigned = 0
            for i in ruido_idx:
                pt = coords[i]
                dist_min, cid_near = float("inf"), None
                for cid, c in cluster_centers:
                    d = _haversine_km(pt, c)
                    if d < dist_min:
                        dist_min, cid_near = d, cid
                if cid_near is not None:
                    labels[i] = cid_near
                    reassigned += 1
                else:
                    # Caso extremo: sem cluster próximo → cria um novo cluster isolado
                    labels[i] = max([cid for cid in set(labels) if cid != -1], default=0) + 1
            logger.info(f"🔁 {reassigned} PDVs de ruído reatribuídos ao cluster mais próximo.")


    # =======================================================
    # 🏁 Recalcula clusters com todos os PDVs
    # =======================================================
    setores: List[Setor] = []
    for cid in sorted(set(labels)):
        idx = np.where(labels == cid)[0]
        pts = [coords[i] for i in idx]
        c = (
            float(np.mean([p[0] for p in pts])),
            float(np.mean([p[1] for p in pts])),
        )
        med, p95 = _raios_cluster(c, pts)
        setores.append(
            Setor(
                cluster_label=int(cid),
                centro_lat=c[0],
                centro_lon=c[1],
                n_pdvs=int(len(idx)),
                raio_med_km=float(med),
                raio_p95_km=float(p95),
            )
        )

    elapsed = round(time.time() - start, 2)
    logger.info(f"✅ DBSCAN concluído com {len(setores)} clusters válidos (sem PDVs fora) em {elapsed}s.")
    return setores, labels

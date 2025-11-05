#sales_router/src/sales_clusterization/domain/sector_generator.py

# ==========================================================
# 📦 src/sales_clusterization/domain/sector_generator.py
# ==========================================================

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

RANDOM_STATE = 42  # 🔒 garante reprodutibilidade entre execuções


# ==========================================================
# 🔹 Funções auxiliares
# ==========================================================
def _raios_cluster(centro: Tuple[float, float], pts: List[Tuple[float, float]]):
    """Calcula mediana e percentil 95 das distâncias dos pontos ao centro."""
    if not pts:
        return 0.0, 0.0
    dists = [_haversine_km(centro, p) for p in pts]
    dists.sort()
    med = dists[len(dists) // 2]
    p95 = dists[int(0.95 * len(dists)) - 1] if len(dists) >= 2 else dists[-1]
    return med, p95


# ==========================================================
# 🧠 KMEANS com paralelismo interno e centros reais (compatível PDV + MKP)
# ==========================================================
def kmeans_setores(pdvs: List[PDV], k: int, random_state: int = RANDOM_STATE):
    """
    Executa KMeans com paralelismo interno via joblib.
    - Compatível com pipelines de PDV e Marketplace.
    - Retorna setores com coordenadas e, se disponível, lista de PDVs (para avaliação operacional).
    """
    if not pdvs:
        return [], []

    start = time.time()

    coords = [(p.lat, p.lon) for p in pdvs if p.lat and p.lon and not np.isnan(p.lat) and not np.isnan(p.lon)]
    if len(coords) < 2:
        logger.warning(f"⚠️ Dados insuficientes para KMeans ({len(coords)} pontos válidos).")
        return [], []

    X = np.array(coords, dtype=np.float64)
    cpu_cores = psutil.cpu_count(logical=True)

    logger.info(f"🧮 Executando KMeans (K={k}) com {cpu_cores} núcleos (loky backend)...")

    km = KMeans(n_clusters=k, random_state=random_state, n_init="auto", algorithm="lloyd")

    with parallel_backend("loky", n_jobs=-1):
        labels = km.fit_predict(X)

    centers = km.cluster_centers_

    setores: List[Setor] = []
    for cid in range(k):
        idx = np.where(labels == cid)[0]
        cluster_pdvs = [pdvs[i] for i in idx]
        pts = [(p.lat, p.lon) for p in cluster_pdvs]

        if not pts:
            logger.warning(f"⚠️ Cluster {cid} vazio — ignorado.")
            continue

        centro_lat, centro_lon = map(float, centers[cid])
        med, p95 = _raios_cluster((centro_lat, centro_lon), pts)

        # ✅ Cria entidade compatível para ambos os casos
        setores.append(
            Setor(
                cluster_label=int(cid),
                centro_lat=centro_lat,
                centro_lon=centro_lon,
                n_pdvs=int(len(cluster_pdvs)),
                raio_med_km=float(med),
                raio_p95_km=float(p95),
                metrics={
                    "centro_tipo": "kmeans",
                    "raio_med_km": float(med),
                    "raio_p95_km": float(p95),
                },
                # 👇 atributos opcionais (compatíveis com PDV e MKP)
                pdvs=cluster_pdvs if cluster_pdvs else None,
                coords=pts if pts else None,
            )
        )

        logger.debug(
            f"📍 Cluster {cid}: {len(cluster_pdvs)} pontos | "
            f"Centro ({centro_lat:.6f}, {centro_lon:.6f}) | "
            f"Raio med={med:.2f} km | P95={p95:.2f} km"
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
    Executa DBSCAN com ajuste automático de eps_km e min_samples com base na densidade média.
    🔹 Versão refinada:
        - Ajusta min_samples dinamicamente para bases pequenas
        - Loga fallback explícito (cluster único)
        - Adiciona diagnóstico detalhado no log
    """
    if not pdvs:
        return [], []

    start = time.time()
    coords = np.array([[p.lat, p.lon] for p in pdvs if p.lat and p.lon], dtype=np.float64)
    n = len(coords)
    if n < 2:
        logger.warning("⚠️ Dados insuficientes para DBSCAN.")
        return [], []

    # 🔍 Densidade média
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

    if eps_km is None:
        eps_km = np.clip(med_dist_km * 20, eps_min_km, eps_max_km)

    if min_samples is None:
        if n < 60:
            min_samples = max(3, min(30, n // 2))
            origem = "adaptativo (base pequena)"
        else:
            densidade_rel = 1 / (med_dist_km + 1e-6)
            densidade_norm = np.clip((densidade_rel - 0.05) / 0.1, 0, 1)
            min_samples = int(min_samples_min + densidade_norm * (min_samples_max - min_samples_min))
            min_samples = int(np.clip(min_samples, min_samples_min, min_samples_max))
            origem = "densidade"
    else:
        origem = "forçado"

    logger.info(
        f"📏 eps_km ajustado: {eps_km:.2f} km | min_samples={min_samples} ({origem}) | "
        f"densidade média={med_dist_km:.2f} km | total PDVs={n}"
    )

    eps_deg = eps_km / 111.0
    db = DBSCAN(eps=eps_deg, min_samples=min_samples)
    labels = db.fit_predict(coords)

    n_clusters = len(set(labels)) - (1 if -1 in labels else 0)
    ruido_count = np.sum(labels == -1)
    logger.info(f"📊 DBSCAN terminou com {n_clusters} clusters e {ruido_count} PDVs de ruído.")

    if n_clusters == 0:
        labels[:] = 0
        logger.warning("⚠️ Nenhum cluster DBSCAN formado — fallback aplicado → cluster único.")
    elif ruido_count > 0:
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
        logger.info(f"🔁 {reassigned} PDVs de ruído reatribuídos ao cluster mais próximo.")

    setores: List[Setor] = []
    for cid in sorted(set(labels)):
        idx = np.where(labels == cid)[0]
        pts = [coords[i] for i in idx]
        c = (float(np.mean([p[0] for p in pts])), float(np.mean([p[1] for p in pts])))
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
    logger.info(f"✅ DBSCAN concluído com {len(setores)} clusters válidos em {elapsed}s.")
    return setores, labels


# ==========================================================
# ⚖️ KMEANS balanceado com refinamento adaptativo por limites
# ==========================================================
def kmeans_balanceado(pdvs: List[PDV], max_pdv_cluster: int, v_kmh: float, max_dist_km: float, max_time_min: float, tempo_servico_min: float):
    """
    Executa KMeans balanceado:
    - Define k inicial = ceil(N / max_pdv_cluster)
    - Distribui PDVs de forma homogênea em clusters equilibrados
    - Ajusta clusters que violam tempo/distância, subdividindo-os recursivamente
    """
    from sklearn.cluster import KMeans
    import numpy as np
    import math
    from src.sales_clusterization.domain.k_estimator import _haversine_km

    def avaliar_cluster(coords, centro):
        """Retorna distância total (km) e tempo total (min)."""
        if len(coords) == 0:
            return 0, 0
        # Rota simulada aproximada (vizinho mais próximo simples)
        dist_total = 0
        atual = np.array(centro)
        visitados = np.zeros(len(coords), dtype=bool)
        for _ in range(len(coords)):
            dists = np.array([_haversine_km(atual, p) if not v else np.inf for p, v in zip(coords, visitados)])
            idx = np.argmin(dists)
            dist_total += dists[idx]
            atual = coords[idx]
            visitados[idx] = True
        dist_total += _haversine_km(atual, centro)
        tempo_total = (dist_total / max(v_kmh, 1e-3)) * 60 + len(coords) * tempo_servico_min
        return dist_total, tempo_total

    def subdividir(coords, k):
        """Executa KMeans sobre coordenadas e retorna subclusters [(subcoords, centro), ...]."""
        X = np.array(coords, dtype=np.float64)
        km = KMeans(n_clusters=k, init="k-means++", random_state=42, n_init=10)
        labels = km.fit_predict(X)
        subs = []
        for i in range(k):
            subcoords = X[labels == i]
            if len(subcoords) == 0:
                continue
            centro = tuple(map(float, km.cluster_centers_[i]))
            subs.append((subcoords, centro))
        return subs

    def refinar_recursivo(coords):
        """Avalia e subdivide clusters até atender limites operacionais."""
        centro = tuple(map(float, np.mean(coords, axis=0)))
        dist_km, tempo_min = avaliar_cluster(coords, centro)
        if (dist_km > max_dist_km or tempo_min > max_time_min) and len(coords) > max_pdv_cluster:
            k_sub = math.ceil(len(coords) / max_pdv_cluster)
            logger.warning(f"⚠️ Cluster excede limites ({dist_km:.1f} km / {tempo_min:.1f} min) → subdividindo em {k_sub}")
            resultado = []
            for subcoords, _ in subdividir(coords, k_sub):
                resultado.extend(refinar_recursivo(subcoords))
            return resultado
        else:
            return [(coords, centro, dist_km, tempo_min)]

    # ==========================================================
    # 🔹 Etapa inicial: clusterização balanceada global
    # ==========================================================
    n = len(pdvs)
    if n == 0:
        return []

    k_inicial = max(1, math.ceil(n / max_pdv_cluster))
    logger.info(f"⚙️ Iniciando KMeans balanceado (N={n}, max_pdv_cluster={max_pdv_cluster}, k_inicial={k_inicial})")

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    kmeans = KMeans(n_clusters=k_inicial, init="k-means++", random_state=42, n_init=10)
    labels = kmeans.fit_predict(coords)

    clusters_refinados = []
    for i in range(k_inicial):
        subset = coords[labels == i]
        if len(subset) == 0:
            continue
        clusters_refinados.extend(refinar_recursivo(subset))

    # Converte clusters em entidades Setor
    setores = []
    for idx, (coords_sub, centro, dist_km, tempo_min) in enumerate(clusters_refinados):
        pts = [(float(x[0]), float(x[1])) for x in coords_sub]
        med, p95 = _raios_cluster(centro, pts)

        # 🆕 Mapeia PDVs originais correspondentes
        pdvs_sub = []
        for p in pdvs:
            if any(abs(p.lat - c[0]) < 1e-6 and abs(p.lon - c[1]) < 1e-6 for c in pts):
                p.cluster_label = idx            # 🔹 <<< adiciona esta linha
                pdvs_sub.append(p)

        setores.append(
            Setor(
                cluster_label=idx,
                centro_lat=centro[0],
                centro_lon=centro[1],
                n_pdvs=len(coords_sub),
                raio_med_km=med,
                raio_p95_km=p95,
                pdvs=pdvs_sub,  # ✅ garante compatibilidade com refinador
                coords=pts,
                metrics={"dist_km": dist_km, "tempo_min": tempo_min},
            )
        )

    logger.success(f"✅ KMeans balanceado concluído: {len(setores)} clusters finais.")
    return setores

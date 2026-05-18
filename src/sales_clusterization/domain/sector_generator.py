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

    km = KMeans(n_clusters=k, random_state=random_state, n_init=10, algorithm="lloyd")


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
def kmeans_fixo(pdvs: List[PDV], k: int):
    """
    Roda KMeans simples com K fixo definido pelo usuário (modo "Número fixo").
    Sem balanceamento, sem subdivisão, sem refinamento — entrega exatamente K
    setores, com tamanhos que podem variar livremente conforme a geografia.
    """
    import numpy as np
    from sklearn.cluster import KMeans
    import math  # noqa: F401

    n = len(pdvs)
    if n == 0:
        return []

    k_efetivo = max(1, min(int(k), n))
    if k_efetivo != k:
        logger.warning(
            f"⚠️ K solicitado={k} ajustado para {k_efetivo} "
            f"(precisa estar entre 1 e N={n})."
        )

    logger.info(f"🎯 KMeans fixo | N={n}, K={k_efetivo}")

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    km = KMeans(n_clusters=k_efetivo, random_state=42, n_init=10)
    labels = km.fit_predict(coords)

    setores = []
    for cid in range(k_efetivo):
        mask = labels == cid
        if not mask.any():
            continue
        subset = coords[mask]
        centro = tuple(map(float, np.mean(subset, axis=0)))
        pts = [(float(x[0]), float(x[1])) for x in subset]
        med, p95 = _raios_cluster(centro, pts)
        pdvs_sub = [p for p, m in zip(pdvs, mask) if m]
        for p in pdvs_sub:
            p.cluster_label = cid

        setores.append(
            Setor(
                cluster_label=cid,
                centro_lat=centro[0],
                centro_lon=centro[1],
                n_pdvs=len(pdvs_sub),
                raio_med_km=med,
                raio_p95_km=p95,
                pdvs=pdvs_sub,
                coords=pts,
                metrics={"modo": "fixo"},
            )
        )

    logger.success(
        f"✅ KMeans fixo concluído | K={len(setores)} | "
        f"tamanhos={[s.n_pdvs for s in setores]}"
    )
    return setores


def kmeans_balanceado(
    pdvs: List[PDV],
    max_pdv_cluster: int,
    v_kmh: float,
    max_dist_km: float,
    max_time_min: float,
    tempo_servico_min: float,
    redistribuir: bool = False,
):
    """
    Executa KMeans balanceado com controle de tamanho máximo de cluster.
    1️⃣ Calcula K inicial = ceil(N / max_pdv_cluster)
    2️⃣ Executa KMeans normal
    3️⃣ Se redistribuir=True, tenta redistribuir pontos excedentes do
        cluster cheio pros vizinhos com folga antes de subdividir
        (preserva o K mínimo teórico — pedido no modo "capacidade").
    4️⃣ Subdivide clusters que ainda excedam o limite de PDVs
    5️⃣ Só depois calcula rotas simuladas (Haversine) e tempos
    """
    import numpy as np
    from sklearn.cluster import KMeans
    import math
    from src.sales_clusterization.domain.k_estimator import _haversine_km

    def subdividir_por_tamanho(coords, indices_originais, max_pdv_cluster):
        if len(coords) <= max_pdv_cluster:
            return [(coords, indices_originais)]

        k_sub = math.ceil(len(coords) / max_pdv_cluster)
        km = KMeans(n_clusters=k_sub, random_state=42, n_init=10).fit(coords)
        labels_sub = km.labels_

        resultados = []
        for sub_id in range(k_sub):
            mask = labels_sub == sub_id
            if not np.any(mask):
                continue

            sub_coords = coords[mask].astype(np.float64)
            sub_idx = indices_originais[mask]
            resultados.append((sub_coords, sub_idx))


        logger.warning(f"⚠️ Cluster excedido ({len(coords)} PDVs) → subdividido em {len(resultados)} partes.")
        return resultados


    def avaliar_cluster(coords, centro):
        """Calcula distância total e tempo de rota simulada (vizinho mais próximo simples)."""
        if len(coords) == 0:
            return 0.0, 0.0
        dist_total = 0.0
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

    # ==========================================================
    # 🔹 Etapa 1: K inicial pelo número máximo de PDVs
    # ==========================================================
    n = len(pdvs)
    if n == 0:
        return []

    k_inicial = max(1, math.ceil(n / max_pdv_cluster))

    # 🔒 Garante que bases maiores que o limite gerem pelo menos 2 clusters
    if n > max_pdv_cluster and k_inicial == 1:
        logger.warning(
            f"⚠️ Total de PDVs ({n}) excede o limite ({max_pdv_cluster}), "
            "mas K inicial foi 1 — ajustando para K=2."
        )
        k_inicial = 2

    logger.info(f"⚙️ Iniciando KMeans balanceado (N={n}, max_pdv_cluster={max_pdv_cluster}, k_inicial={k_inicial})")

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    kmeans = KMeans(n_clusters=k_inicial, random_state=42, n_init=10)
    labels = kmeans.fit_predict(coords)

    # ==========================================================
    # 🔹 Etapa 1.5: Redistribuição (modo capacidade)
    # ----------------------------------------------------------
    # Sem isso: clusters cheios são subdivididos → K final cresce.
    # Com isso: pontos mais distantes do centro do cluster cheio
    # migram pra clusters vizinhos com folga, preservando K mínimo.
    # Repete até convergir ou não conseguir mais mover (safety:
    # max_pdv_cluster * k_inicial iterações).
    # ==========================================================
    if redistribuir:
        centros = kmeans.cluster_centers_.copy()
        labels = labels.copy()

        # Razão máxima dist(destino)/dist(origem) tolerada pra mover um ponto.
        # Se passar disso, o ponto "pertence" geograficamente ao cluster cheio
        # e movê-lo deixaria buraco visível no mapa (ilha laranja dentro do
        # azul). Aborta o rebalanceamento e cai pro fallback de subdivisão.
        LIMITE_RAZAO = 2.0

        # Cap = N (cada iteração move 1 ponto).
        for _ in range(len(pdvs)):
            sizes = np.bincount(labels, minlength=k_inicial)
            if sizes.max() <= max_pdv_cluster:
                break

            k_full = int(np.argmax(sizes))
            idxs_full = np.where(labels == k_full)[0]

            # Pra cada cluster vizinho com folga, acha o ponto do cheio
            # mais PRÓXIMO do centro do vizinho (= ponto na fronteira).
            # Esse é o melhor candidato a migrar — preserva coesão visual.
            melhor_idx = None
            melhor_k_dest = None
            melhor_razao = float("inf")

            for k_cand in range(k_inicial):
                if k_cand == k_full or sizes[k_cand] >= max_pdv_cluster:
                    continue
                d_destino = np.linalg.norm(
                    coords[idxs_full] - centros[k_cand], axis=1
                )
                i_fronteira = int(np.argmin(d_destino))
                idx_global = idxs_full[i_fronteira]
                d_origem = np.linalg.norm(coords[idx_global] - centros[k_full])
                razao = d_destino[i_fronteira] / max(d_origem, 1e-9)
                if razao < melhor_razao:
                    melhor_razao = razao
                    melhor_idx = idx_global
                    melhor_k_dest = k_cand

            if melhor_idx is None:
                logger.info(
                    "♻️ Redistribuição parou: nenhum cluster com folga. "
                    "Vai cair no fallback de subdivisão."
                )
                break

            if melhor_razao > LIMITE_RAZAO:
                logger.info(
                    f"♻️ Redistribuição abortada: melhor candidato tem "
                    f"razão={melhor_razao:.2f} (>{LIMITE_RAZAO}) — moveria "
                    f"ponto pra cluster geograficamente distante. "
                    f"Vai cair no fallback de subdivisão."
                )
                break

            labels[melhor_idx] = melhor_k_dest

        sizes_final = np.bincount(labels, minlength=k_inicial)
        if sizes_final.max() <= max_pdv_cluster:
            logger.info(
                f"♻️ Redistribuição OK | K preservado={k_inicial} | "
                f"tamanhos={sizes_final.tolist()}"
            )

    # ==========================================================
    # 🔹 Etapa 2: Rebalanceamento por tamanho (AGORA COM ÍNDICES REAIS)
    # ==========================================================
    clusters_validos = []
    clusters_indices = []  # <<<<< ADICIONE ISSO

    for i in range(k_inicial):
        subset = coords[labels == i]
        subset_idx = np.where(labels == i)[0]  # <<< ÍNDICES ORIGINAIS DOS PDVs

        if len(subset) == 0:
            continue

        subclusters = subdividir_por_tamanho(subset, subset_idx, max_pdv_cluster)

        for sub_coords, sub_idx in subclusters:
            clusters_validos.append(sub_coords)
            clusters_indices.append(sub_idx)



    # ==========================================================
    # 🔹 Etapa 3: Avaliação de rota (Haversine + tempo)
    # ==========================================================
    setores = []
    for idx, subset in enumerate(clusters_validos):
        centro = tuple(map(float, np.mean(subset, axis=0)))
        dist_km, tempo_min = avaliar_cluster(subset, centro)
        pts = [(float(x[0]), float(x[1])) for x in subset]
        med, p95 = _raios_cluster(centro, pts)

        # associação correta usando clusters_indices
        real_idx = clusters_indices[idx]
        pdvs_sub = [pdvs[j] for j in real_idx]

        for p in pdvs_sub:
            p.cluster_label = idx


        setores.append(
            Setor(
                cluster_label=idx,
                centro_lat=centro[0],
                centro_lon=centro[1],
                n_pdvs=len(subset),
                raio_med_km=med,
                raio_p95_km=p95,
                pdvs=pdvs_sub,
                coords=pts,
                metrics={
                    "dist_km": dist_km,
                    "tempo_min": tempo_min,
                    "status": "EXCEDIDO"
                    if (dist_km > max_dist_km or tempo_min > max_time_min)
                    else "OK",
                },
            )
        )

    logger.success(f"✅ KMeans balanceado concluído com {len(setores)} clusters finais (todos <= {max_pdv_cluster} PDVs).")
    return setores

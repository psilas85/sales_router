# ==========================================================
# 📦 src/sales_clusterization/domain/sector_generator_hybrid.py
# ==========================================================

import numpy as np
from sklearn.cluster import KMeans, DBSCAN
from loguru import logger
from math import ceil
from .entities import PDV, Setor
from .k_estimator import _haversine_km
from .sector_generator import _raios_cluster

RANDOM_STATE = 42


# ==========================================================
# 🧩 DBSCAN → KMeans → Subclusterização diária iterativa
# ==========================================================
def dbscan_kmeans_balanceado(
    pdvs: list,
    eps_km: float = 1.0,       # 🔹 antes 1.5 — mais sensível
    min_samples: int = 25,     # 🔹 antes 50 — mais sensível
    max_pdv_cluster: int = 200,
    frequencia_visita: int = 1,
    dias_uteis: int = 20,
    workday_min: int = 600,
    tempo_servico_min: int = 30,
    tempo_descarregamento_min: float = 0.4,
    v_kmh: float = 30.0,
):

    """
    Combina DBSCAN e KMeans de forma hierárquica:
      1️⃣ DBSCAN: detecta regiões naturais (macroclusters)
      2️⃣ KMeans: subdivide cada macrocluster conforme max_pdv_cluster e frequencia_visita
      3️⃣ Subclusterização diária iterativa: cria rotas diárias (k_diario = dias_uteis / frequencia)
          - Se tempo_total > workday_min, subdivide novamente (k+1)
    """

    if not pdvs:
        logger.warning("⚠️ Nenhum PDV informado para DBSCAN + KMeans.")
        return [], []

    coords = np.array([[p.lat, p.lon] for p in pdvs if p.lat and p.lon], dtype=np.float64)
    # ==========================================================
    # 🔧 Ajuste adaptativo de sensibilidade do DBSCAN
    # ==========================================================
    if len(pdvs) < 500:
        eps_km, min_samples = 0.8, 15
    elif len(pdvs) < 2000:
        eps_km, min_samples = 1.0, 25
    else:
        eps_km, min_samples = 1.2, 35

    eps_deg = eps_km / 111.0


    # ==========================================================
    # 1️⃣ DBSCAN global (macroclusters)
    # ==========================================================
    db = DBSCAN(eps=eps_deg, min_samples=min_samples)
    labels_db = db.fit_predict(coords)
    n_db_clusters = len(set(labels_db)) - (1 if -1 in labels_db else 0)
    ruido_count = np.sum(labels_db == -1)

    logger.info(f"🧠 DBSCAN detectou {n_db_clusters} clusters válidos ({ruido_count} ruídos).")

    setores_finais = []
    labels_finais = np.full(len(pdvs), -1)

    # ==========================================================
    # 2️⃣ Processamento de cada macrocluster DBSCAN
    # ==========================================================
    for label in sorted(set(labels_db)):
        if label == -1:
            continue

        idx = np.where(labels_db == label)[0]
        cluster_pdvs = [pdvs[i] for i in idx]
        n_cluster = len(cluster_pdvs)
        if n_cluster == 0:
            continue

        logger.info(f"📍 Macrocluster DBSCAN {label}: {n_cluster} PDVs")

        # ======================================================
        # 2.1️⃣ Cálculo do k_interno (KMeans interno)
        # ======================================================
        k_interno = max(1, int(ceil(n_cluster / (max_pdv_cluster / max(1, frequencia_visita)))))
        logger.info(
            f"🔹 Subdividindo cluster {label} via KMeans (k_interno={k_interno}) "
            f"| max_pdv_cluster={max_pdv_cluster} | freq={frequencia_visita}"
        )

        X_local = np.array([[p.lat, p.lon] for p in cluster_pdvs], dtype=np.float64)
        km = KMeans(n_clusters=k_interno, random_state=RANDOM_STATE, n_init="auto")
        km_labels = km.fit_predict(X_local)
        centers = km.cluster_centers_

        # ======================================================
        # 3️⃣ Subclusterização diária iterativa (rotas)
        # ======================================================
        for k in range(k_interno):
            idx_sub = np.where(km_labels == k)[0]
            pdvs_sub = [cluster_pdvs[i] for i in idx_sub]
            pts = [(p.lat, p.lon) for p in pdvs_sub]
            if not pts:
                continue

            centro_lat, centro_lon = map(float, centers[k])
            med, p95 = _raios_cluster((centro_lat, centro_lon), pts)
            n_pdvs_sub = len(pdvs_sub)

            # ==================================================
            # 3.1️⃣ Cálculo do número de microclusters (rotas diárias)
            # ==================================================
            k_diario = max(1, int(ceil(dias_uteis / max(1, frequencia_visita))))
            # ✅ Proteção contra clusters pequenos
            k_diario = min(k_diario, len(pts))

            logger.info(f"🕒 Cluster {label}-{k}: criando {k_diario} rotas diárias iniciais...")

            # ==================================================
            # 3.2️⃣ Avaliação iterativa por tempo e distância
            # ==================================================
            iteracao = 0
            tempo_estimado = 99999.0
            while tempo_estimado > workday_min and iteracao < 10:
                km_daily = KMeans(n_clusters=k_diario, random_state=RANDOM_STATE, n_init="auto").fit(pts)
                centers_daily = km_daily.cluster_centers_
                tempo_total = []
                dist_total = []

                for i in range(k_diario):
                    pts_daily = [pts[j] for j in range(len(pts)) if km_daily.labels_[j] == i]
                    if len(pts_daily) < 2:
                        continue

                    # ✅ Correção: _haversine_km recebe tuplas (lat, lon)
                    dist_km = np.mean(
                        [_haversine_km(pts_daily[j], pts_daily[j - 1]) for j in range(1, len(pts_daily))]
                    )

                    tempo_transito = (dist_km / max(v_kmh, 1)) * 60
                    tempo_paradas = len(pts_daily) * tempo_servico_min
                    tempo_descarregamento = len(pts_daily) * tempo_descarregamento_min
                    tempo_total_min = tempo_transito + tempo_paradas + tempo_descarregamento

                    tempo_total.append(tempo_total_min)
                    dist_total.append(dist_km)

                tempo_estimado = np.mean(tempo_total) if tempo_total else 0
                logger.debug(
                    f"🔁 Iter {iteracao} | Cluster {label}-{k} | "
                    f"Rotas={k_diario} | Tempo médio={tempo_estimado:.1f} min"
                )

                if tempo_estimado > workday_min:
                    k_diario += 1
                    iteracao += 1
                else:
                    break

            # ==================================================
            # 3.3️⃣ Registro final do setor
            # ==================================================
            setor = Setor(
                cluster_label=int(len(setores_finais)),
                centro_lat=centro_lat,
                centro_lon=centro_lon,
                n_pdvs=n_pdvs_sub,
                raio_med_km=med,
                raio_p95_km=p95,
                pdvs=pdvs_sub,
                coords=pts,
            )
            setores_finais.append(setor)

            for i in idx[idx_sub]:
                labels_finais[i] = setor.cluster_label

    logger.success(
        f"✅ DBSCAN → KMeans finalizado: {len(setores_finais)} clusters "
        f"(freq={frequencia_visita}, dias_uteis={dias_uteis})"
    )
    return setores_finais, labels_finais

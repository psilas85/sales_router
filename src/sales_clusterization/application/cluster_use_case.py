# ============================================================
# 📦 src/sales_clusterization/application/cluster_use_case.py
# ============================================================

from typing import Optional, Dict, Any, List
from loguru import logger
import numpy as np
from sklearn.neighbors import NearestNeighbors

from src.sales_clusterization.infrastructure.persistence.database_reader import carregar_pdvs
from src.sales_clusterization.infrastructure.persistence.database_writer import (
    criar_run,
    finalizar_run,
    salvar_setores,
    salvar_mapeamento_pdvs,
    salvar_outliers,
)
from src.sales_clusterization.infrastructure.logging.run_logger import snapshot_params

from src.sales_clusterization.domain.entities import PDV, Setor
from src.sales_clusterization.domain.k_estimator import estimar_k_inicial
from src.sales_clusterization.domain.operational_cluster_refiner import OperationalClusterRefiner

# 🔵 KMeans balanceado (teto por cluster)
from src.sales_clusterization.domain.sector_generator import kmeans_balanceado

# 🟢 Sweep com capacidade
from src.sales_clusterization.domain.capacitated_sweep import capacitated_sweep
from src.sales_clusterization.domain.dense_subset import dense_subset



# ============================================================
# 🧠 Detecção simplificada de outliers geográficos
# ============================================================
def detectar_outliers_geograficos(pdvs: List[PDV], z_thresh: float = 1.5):
    if len(pdvs) < 5:
        return [(p, False) for p in pdvs]

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    coords_rad = np.radians(coords)

    nn = NearestNeighbors(n_neighbors=min(6, len(coords)), metric="haversine")
    nn.fit(coords_rad)

    dist, _ = nn.kneighbors(coords_rad)
    dist_min = dist[:, 1] * 6371.0  # km

    mean = np.mean(dist_min)
    std = np.std(dist_min)

    limiar = mean + z_thresh * std
    flags = dist_min > limiar

    logger.info(f"🧹 Outliers detectados={np.sum(flags)}/{len(pdvs)} | limiar={limiar:.2f} km")
    return [(pdvs[i], bool(flags[i])) for i in range(len(pdvs))]


# ============================================================
# 🚀 Execução principal
# ============================================================
def executar_clusterizacao(
    tenant_id: int,
    uf: Optional[str],
    cidade: Optional[str],
    algo: str,
    dias_uteis: int,
    freq: int,
    workday_min: int,
    route_km_max: float,
    service_min: int,
    v_kmh: float,
    alpha_path: float,
    max_pdv_cluster: int,
    descricao: str,
    input_id: str,
    clusterization_id: str,
    excluir_outliers: bool,
    z_thresh: float,
    max_iter: int,
) -> Dict[str, Any]:

    logger.info(f"🏁 Iniciando clusterização | tenant={tenant_id} | algo={algo}")

    # ============================================================
    # 1) Carregar PDVs
    # ============================================================
    pdvs = carregar_pdvs(tenant_id, input_id, uf, cidade)
    if not pdvs:
        raise ValueError("Nenhum PDV encontrado.")

    logger.info(f"📦 {len(pdvs)} PDVs carregados.")

    # ============================================================
    # 1.5) Remover duplicados (evita falso outlier)
    # ============================================================
    pdvs = list({(p.lat, p.lon, p.cnpj): p for p in pdvs}.values())

    # ============================================================
    # 2) Outliers
    # ============================================================
    pdv_flags = detectar_outliers_geograficos(pdvs, z_thresh)
    total_outliers = sum(1 for _, f in pdv_flags if f)

    salvar_outliers(
        tenant_id,
        clusterization_id,
        [
            {
                "pdv_id": getattr(p, "id", None),
                "lat": p.lat,
                "lon": p.lon,
                "is_outlier": bool(flag),
            }
            for p, flag in pdv_flags
        ],
    )

    if excluir_outliers:
        pdvs = [p for p, f in pdv_flags if not f]
        logger.info(f"🧹 {total_outliers} outliers removidos.")

    # ============================================================
    # 3) Registrar execução
    # ============================================================
    params = snapshot_params(
        uf=uf,
        cidade=cidade,
        algo=algo,
        k_forcado=None,
        dias_uteis=dias_uteis,
        freq=freq,
        workday_min=workday_min,
        route_km_max=route_km_max,
        service_min=service_min,
        v_kmh=v_kmh,
        alpha_path=alpha_path,
        n_pdvs=len(pdvs),
        max_pdv_cluster=max_pdv_cluster,
        descricao=descricao,
        input_id=input_id,
        clusterization_id=clusterization_id,
    )

    run_id = criar_run(
        tenant_id=tenant_id,
        uf=uf,
        cidade=cidade,
        algo=algo,
        params=params,
        descricao=descricao,
        input_id=input_id,
        clusterization_id=clusterization_id,
    )

    try:
        # ============================================================
        # 🔵 KMEANS — único com refinamento operacional
        # ============================================================
        if algo == "kmeans":
            logger.info("🔵 Executando KMEANS + refinamento.")

            k0, _ = estimar_k_inicial(
                pdvs=pdvs,
                workday_min=workday_min,
                route_km_max=route_km_max,
                service_min=service_min,
                v_kmh=v_kmh,
                dias_uteis=dias_uteis,
                freq=freq,
                max_pdv_cluster=max_pdv_cluster,
                alpha_path=alpha_path,
            )

            setores = kmeans_balanceado(
                pdvs=pdvs,
                max_pdv_cluster=max_pdv_cluster,
                v_kmh=v_kmh,
                max_dist_km=route_km_max,
                max_time_min=workday_min,
                tempo_servico_min=service_min,
            )

            refiner = OperationalClusterRefiner(
                v_kmh=v_kmh,
                max_time_min=workday_min,
                max_dist_km=route_km_max,
                tempo_servico_min=service_min,
                max_iter=max_iter,
                tenant_id=tenant_id,
            )

            setores_finais = refiner.refinar_com_subclusters_iterativo(
                pdvs=pdvs,
                dias_uteis=dias_uteis,
                freq=freq,
                max_pdv_cluster=max_pdv_cluster,
            )

        # ============================================================
        # 🟢 CAPACITATED SWEEP
        # ============================================================
        elif algo == "capacitated_sweep":
            logger.info("🟢 Executando CAPACITATED SWEEP.")
            labels, centers = capacitated_sweep(pdvs, max_capacity=max_pdv_cluster)

            setores_finais = []
            for cid, c in enumerate(centers):
                cluster_points = [p for p, lbl in zip(pdvs, labels) if lbl == cid]
                if not cluster_points:
                    continue

                setores_finais.append(
                    Setor(
                        cluster_label=cid,
                        centro_lat=float(c[0]),
                        centro_lon=float(c[1]),
                        n_pdvs=len(cluster_points),
                        raio_med_km=0,
                        raio_p95_km=0,
                    )
                )
                for p in cluster_points:
                    p.cluster_label = cid

        # ============================================================
        # 🟣 DENSE SUBSET — cluster único, compacto
        # ============================================================
        elif algo == "dense_subset":
            logger.info(f"🟣 Executando DENSE SUBSET | capacidade={max_pdv_cluster}")

            selecionados = dense_subset(pdvs, capacidade=max_pdv_cluster)

            # centro = média dos selecionados
            lat_med = float(np.mean([p.lat for p in selecionados]))
            lon_med = float(np.mean([p.lon for p in selecionados]))

            # Criar setor único
            setores_finais = [
                Setor(
                    cluster_label=0,
                    centro_lat=lat_med,
                    centro_lon=lon_med,
                    n_pdvs=len(selecionados),
                    raio_med_km=0,
                    raio_p95_km=0,
                )
            ]

            # marcar cluster_label nos PDVs selecionados
            for p in selecionados:
                p.cluster_label = 0

            # ⚠️ Só esses PDVs vão ser persistidos
            pdvs = selecionados


        # ============================================================
        # 💾 Persistência
        # ============================================================

        # 🔑 Normalização única e consistente
        labels_orig = sorted({s.cluster_label for s in setores_finais})
        mapa = {old: new for new, old in enumerate(labels_orig)}

        for s in setores_finais:
            s.cluster_label = mapa[s.cluster_label]

        for p in pdvs:
            if p.cluster_label in mapa:
                p.cluster_label = mapa[p.cluster_label]

        mapping = salvar_setores(tenant_id, run_id, setores_finais)

        for p in pdvs:
            p.cluster_id = mapping[p.cluster_label]

        salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)

        finalizar_run(run_id, status="done", k_final=len(setores_finais))

        return {
            "tenant_id": tenant_id,
            "clusterization_id": clusterization_id,
            "run_id": run_id,
            "algo": algo,
            "k_final": len(setores_finais),
            "n_pdvs": len(pdvs),
            "outliers": total_outliers,
            "setores": [
                {
                    "cluster_label": s.cluster_label,
                    "centro_lat": s.centro_lat,
                    "centro_lon": s.centro_lon,
                    "n_pdvs": s.n_pdvs,
                }
                for s in setores_finais
            ],
        }

    except Exception as e:
        logger.error(f"❌ Erro durante clusterização: {e}")
        finalizar_run(run_id, status="error", k_final=0, error=str(e))
        raise

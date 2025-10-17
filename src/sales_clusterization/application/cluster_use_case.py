# src/sales_clusterization/application/cluster_use_case.py

from typing import Optional, Dict, Any
from loguru import logger
from src.sales_clusterization.infrastructure.persistence.database_reader import carregar_pdvs
from src.sales_clusterization.infrastructure.persistence.database_writer import (
    criar_run,
    finalizar_run,
    salvar_setores,
    salvar_mapeamento_pdvs,
)
from src.sales_clusterization.infrastructure.logging.run_logger import snapshot_params
from src.sales_clusterization.domain.k_estimator import estimar_k_inicial
from src.sales_clusterization.domain.sector_generator import kmeans_setores, dbscan_setores
from src.sales_clusterization.domain.sector_generator_hybrid import dbscan_kmeans_balanceado
from src.sales_clusterization.domain.validators import checar_raio


def executar_clusterizacao(
    tenant_id: int,
    uf: Optional[str] = None,
    cidade: Optional[str] = None,
    algo: str = "kmeans",
    k_forcado: Optional[int] = None,
    dias_uteis: int = 20,
    freq: int = 1,
    workday_min: int = 480,
    route_km_max: float = 150.0,
    service_min: int = 20,
    v_kmh: float = 30.0,
    alpha_path: float = 1.4,
    max_pdv_cluster: int = 300,
) -> Dict[str, Any]:
    """
    Executa o fluxo completo de clusterização:
    - DBSCAN híbrido com balanceamento via KMeans
    - Mantém 100% dos PDVs (reatribuição de ruído)
    - Garante que clusters não ultrapassem max_pdv_cluster PDVs
    """

    logger.info(f"🏁 Iniciando clusterização | tenant_id={tenant_id} | {uf}-{cidade} | algoritmo={algo}")

    # 1️⃣ Carrega PDVs
    pdvs = carregar_pdvs(tenant_id=tenant_id, uf=uf, cidade=cidade)
    if not pdvs:
        raise ValueError(f"Nenhum PDV encontrado para tenant_id={tenant_id} nos filtros {uf}-{cidade}.")
    logger.info(f"✅ {len(pdvs)} PDVs carregados.")

    # 2️⃣ Snapshot
    params = snapshot_params(
        uf=uf,
        cidade=cidade,
        algo=algo,
        k_forcado=k_forcado,
        dias_uteis=dias_uteis,
        freq=freq,
        workday_min=workday_min,
        route_km_max=route_km_max,
        service_min=service_min,
        v_kmh=v_kmh,
        alpha_path=alpha_path,
        n_pdvs=len(pdvs),
        max_pdv_cluster=max_pdv_cluster,
    )

    # 3️⃣ Run
    run_id = criar_run(tenant_id, uf, cidade, algo, params)
    logger.info(f"🆕 Execução registrada (run_id={run_id}).")

    try:
        if algo == "kmeans":
            # --------------------------------------------
            # Modo clássico KMeans
            # --------------------------------------------
            if k_forcado:
                k0 = k_forcado
                diag = {"modo": "forçado"}
            else:
                k0, diag = estimar_k_inicial(
                    pdvs, workday_min, route_km_max, service_min, v_kmh, dias_uteis, freq, alpha_path
                )

            setores, labels = kmeans_setores(pdvs, k0)
            if not checar_raio(setores, route_km_max):
                k_ref = int(round(k0 * 1.1))
                setores, labels = kmeans_setores(pdvs, k_ref)
                k0 = k_ref
                diag["ajuste_raio"] = k_ref

        elif algo == "dbscan":
            # --------------------------------------------
            # Modo híbrido DBSCAN + KMeans balanceador
            # --------------------------------------------
            logger.info(f"🔹 Executando DBSCAN híbrido (limite={max_pdv_cluster} PDVs por cluster)...")
            setores, labels = dbscan_kmeans_balanceado(pdvs, max_pdv_cluster=max_pdv_cluster)
            k0 = len(setores)
            diag = {"dbscan_k": k0, "balanceado": True}

        else:
            raise ValueError("Algoritmo não suportado. Use 'kmeans' ou 'dbscan'.")

        # 4️⃣ Salvar resultados
        mapping = salvar_setores(tenant_id, run_id, setores)
        salvar_mapeamento_pdvs(tenant_id, run_id, mapping, labels, pdvs)
        logger.info("✅ Clusterização salva no banco.")

        # 5️⃣ Finalizar run
        finalizar_run(run_id, k_final=k0, status="done")
        logger.success(f"🏁 Clusterização concluída | run_id={run_id} | K={k0}")

        return {
            "tenant_id": tenant_id,
            "run_id": run_id,
            "algo": algo,
            "k_final": k0,
            "n_pdvs": len(pdvs),
            "diagnostico": diag,
            "setores": [
                {
                    "cluster_label": s.cluster_label,
                    "centro_lat": s.centro_lat,
                    "centro_lon": s.centro_lon,
                    "n_pdvs": s.n_pdvs,
                    "raio_med_km": s.raio_med_km,
                    "raio_p95_km": s.raio_p95_km,
                }
                for s in setores
            ],
        }

    except Exception as e:
        logger.error(f"❌ Erro durante clusterização (run_id={run_id}): {e}")
        finalizar_run(run_id, k_final=0, status="error", error=str(e))
        raise

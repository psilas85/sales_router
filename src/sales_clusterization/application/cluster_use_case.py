# src/sales_clusterization/application/cluster_use_case.py

# ============================================================
# 📦 src/sales_clusterization/application/cluster_use_case.py
# ============================================================

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
from src.sales_clusterization.domain.sector_generator import kmeans_setores
from src.sales_clusterization.domain.sector_generator_hybrid import dbscan_kmeans_balanceado
from src.sales_clusterization.domain.validators import checar_raio


def executar_clusterizacao(
    tenant_id: int,
    uf: Optional[str],
    cidade: Optional[str],
    algo: str,
    k_forcado: Optional[int],
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
) -> Dict[str, Any]:
    """
    Executa o fluxo completo de clusterização:
    - Usa o input_id informado para buscar PDVs da base correta
    - Cria um registro histórico no banco com clusterization_id e descrição
    - Executa o algoritmo (KMeans ou DBSCAN balanceado)
    - Persiste resultados completos sem sobrescrever dados anteriores
    """

    logger.info(
        f"🏁 Iniciando clusterização | tenant_id={tenant_id} | {uf}-{cidade} "
        f"| algo={algo} | input_id={input_id} | clusterization_id={clusterization_id}"
    )

    # 1️⃣ Carrega PDVs da base vinculada ao input_id
    pdvs = carregar_pdvs(tenant_id=tenant_id, input_id=input_id, uf=uf, cidade=cidade)
    if not pdvs:
        raise ValueError(
            f"Nenhum PDV encontrado para tenant_id={tenant_id}, input_id={input_id}, filtros={uf}-{cidade}."
        )
    logger.info(f"✅ {len(pdvs)} PDVs carregados (input_id={input_id}).")

    # 2️⃣ Snapshot de parâmetros
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
        descricao=descricao,
        input_id=input_id,
        clusterization_id=clusterization_id,
    )

    # 3️⃣ Cria registro de execução
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
    logger.info(f"🆕 Execução registrada | run_id={run_id} | clusterization_id={clusterization_id}")

    try:
        # 4️⃣ Execução do algoritmo
        if algo == "kmeans":
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
            logger.info(f"🔹 Executando DBSCAN híbrido balanceado (limite={max_pdv_cluster} PDVs por cluster)...")
            setores, labels = dbscan_kmeans_balanceado(pdvs, max_pdv_cluster=max_pdv_cluster)
            k0 = len(setores)
            diag = {"dbscan_k": k0, "balanceado": True}

        else:
            raise ValueError("Algoritmo não suportado. Use 'kmeans' ou 'dbscan'.")

        # 5️⃣ Salvar resultados
        mapping = salvar_setores(tenant_id, run_id, setores)
        salvar_mapeamento_pdvs(tenant_id, run_id, mapping, labels, pdvs)
        logger.info(f"✅ Clusterização salva no banco (run_id={run_id}, clusterization_id={clusterization_id}).")

        # 6️⃣ Finalizar run
        finalizar_run(run_id, k_final=k0, status="done")
        logger.success(f"🏁 Clusterização concluída | run_id={run_id} | K={k0}")

        return {
            "tenant_id": tenant_id,
            "clusterization_id": clusterization_id,
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

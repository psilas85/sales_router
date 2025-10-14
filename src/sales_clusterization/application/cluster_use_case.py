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
) -> Dict[str, Any]:
    """
    Executa o fluxo completo de clusterização:
    1. Carrega PDVs filtrados por tenant
    2. Estima K inicial (ou usa K forçado)
    3. Executa clusterização (KMeans ou DBSCAN)
    4. Salva run, setores e mapeamento PDV→setor
    """

    logger.info(f"🏁 Iniciando clusterização | tenant_id={tenant_id} | {uf}-{cidade} | algoritmo={algo}")

    # 1️⃣ Carrega PDVs do tenant
    logger.info("📦 Carregando PDVs do banco de dados ...")
    pdvs = carregar_pdvs(tenant_id=tenant_id, uf=uf, cidade=cidade)
    if not pdvs:
        logger.error(f"Nenhum PDV encontrado para tenant_id={tenant_id} nos filtros {uf}-{cidade}.")
        raise ValueError(f"Nenhum PDV encontrado para tenant_id={tenant_id} nos filtros {uf}-{cidade}.")

    logger.info(f"✅ {len(pdvs)} PDVs carregados com sucesso.")

    # 2️⃣ Snapshot de parâmetros para auditoria
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
    )

    # 3️⃣ Cria run vinculado ao tenant
    run_id = criar_run(tenant_id, uf, cidade, algo, params)
    logger.info(f"🆕 Execução registrada no banco (run_id={run_id}).")

    try:
        # 4️⃣ Determina K inicial
        if algo == "kmeans":
            logger.info("🧮 Iniciando estimativa de K para KMeans...")
            if k_forcado:
                k0 = k_forcado
                diag = {"modo": "forçado"}
                logger.info(f"K forçado definido: {k0}")
            else:
                k0, diag = estimar_k_inicial(
                    pdvs,
                    workday_min,
                    route_km_max,
                    service_min,
                    v_kmh,
                    dias_uteis,
                    freq,
                    alpha_path,
                )
                logger.info(f"K inicial estimado: {k0} (diagnóstico: {diag})")

            logger.info("🔹 Executando KMeans...")
            setores, labels = kmeans_setores(pdvs, k0)
            logger.info(f"✅ KMeans concluído com {len(setores)} clusters gerados.")

            # Checagem de raio — se muito grande, aumenta K levemente
            if not checar_raio(setores, route_km_max):
                k_ref = max(1, int(round(k0 * 1.1)))
                logger.warning(f"⚠️ Raio médio acima do limite ({route_km_max} km). Reajustando K para {k_ref}.")
                setores, labels = kmeans_setores(pdvs, k_ref)
                k0 = k_ref
                diag["ajuste_raio"] = k_ref
                logger.info(f"✅ Novo K aplicado após ajuste de raio: {k0}")

        elif algo == "dbscan":
            logger.info("🔹 Executando DBSCAN...")
            setores, labels = dbscan_setores(pdvs)
            k0 = len(setores)
            diag = {"dbscan_k": k0}
            logger.info(f"✅ DBSCAN gerou {k0} clusters.")

        else:
            raise ValueError("Algoritmo não suportado. Use 'kmeans' ou 'dbscan'.")

        # 5️⃣ Persiste resultados no banco com tenant_id
        logger.info("💾 Salvando setores e mapeamentos PDV→setor...")
        mapping = salvar_setores(tenant_id, run_id, setores)
        salvar_mapeamento_pdvs(tenant_id, run_id, mapping, labels, pdvs)
        logger.info("✅ Dados de clusterização salvos com sucesso.")

        # 6️⃣ Finaliza execução
        finalizar_run(run_id, k_final=k0, status="done")
        logger.success(f"🏁 Clusterização concluída | tenant_id={tenant_id} | run_id={run_id} | K={k0}")

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

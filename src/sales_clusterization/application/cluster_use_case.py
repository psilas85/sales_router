# ============================================================
# 📦 src/sales_clusterization/application/cluster_use_case.py
# ============================================================

from typing import Optional, Dict, Any, List
from loguru import logger
import numpy as np
from sklearn.neighbors import NearestNeighbors
from math import radians, sin, cos, sqrt, atan2

from src.sales_clusterization.infrastructure.persistence.database_reader import carregar_pdvs
from src.sales_clusterization.infrastructure.persistence.database_writer import (
    criar_run,
    finalizar_run,
    salvar_setores,
    salvar_mapeamento_pdvs,
    salvar_outliers,
)
from src.sales_clusterization.infrastructure.logging.run_logger import snapshot_params
from src.sales_clusterization.domain.k_estimator import estimar_k_inicial
from src.sales_clusterization.domain.sector_generator import kmeans_setores
from src.sales_clusterization.domain.sector_generator_hybrid import dbscan_kmeans_balanceado
from src.sales_clusterization.domain.validators import checar_raio
from src.sales_clusterization.domain.entities import PDV


# ============================================================
# 🧠 Detecção de Outliers Geográficos (versão otimizada)
# ============================================================

def detectar_outliers_geograficos(
    pdvs: List[PDV],
    z_thresh: float = 2.0,
    metodo: Optional[str] = None,
    limite_urbano_km: Optional[float] = None,
):
    """
    Detecta outliers geográficos com base na distância ao vizinho mais próximo.
    🔹 Otimizada: usa NearestNeighbors (metric='haversine') para O(N log N)
    🔹 Mantém comportamento híbrido adaptativo e suavizado do código original.
    """
    if len(pdvs) < 5:
        logger.warning("⚠️ Poucos PDVs para detecção de outliers — nenhum removido.")
        return [(p, False) for p in pdvs]

    # Coordenadas e conversão para radianos
    coords = np.array([[p.lat, p.lon] for p in pdvs])
    coords_rad = np.radians(coords)

    # =====================================================
    # 🧭 1️⃣ Densidade local → limite urbano dinâmico
    # =====================================================
    k = min(5, len(coords) - 1)
    nn = NearestNeighbors(n_neighbors=k + 1, metric="haversine")
    nn.fit(coords_rad)
    dist, _ = nn.kneighbors(coords_rad)

    # Distância média até o 5º vizinho (em km)
    media_k5 = np.mean(dist[:, -1]) * 6371.0
    limite_dinamico = np.clip(media_k5 * 7, 4, 12)
    if limite_urbano_km is None:
        limite_urbano_km = limite_dinamico

    # =====================================================
    # 📏 2️⃣ Distância mínima até o vizinho mais próximo
    # =====================================================
    dist_min = dist[:, 1] * 6371.0  # ignora o próprio ponto
    dist_mean = np.mean(dist_min)
    dist_std = np.std(dist_min)
    q1, q3 = np.percentile(dist_min, [25, 75])
    iqr = q3 - q1

    # =====================================================
    # ⚙️ 3️⃣ Seleção automática de método (IQR, Z-score ou híbrido)
    # =====================================================
    if metodo is None:
        if dist_std < 2:
            metodo = "iqr"
        elif dist_std > 5:
            metodo = "zscore"
        else:
            metodo = "hibrido"

    # =====================================================
    # 📊 4️⃣ Limiar de outlier (com suavização)
    # =====================================================
    if metodo == "iqr":
        limiar = q3 + 2.5 * iqr
        metodo_desc = f"IQR adaptativo (Q3 + 2.5*IQR = {limiar:.2f} km)"
    elif metodo == "zscore":
        limiar = dist_mean + z_thresh * dist_std
        metodo_desc = f"Z-score adaptativo (μ + {z_thresh}σ = {limiar:.2f} km)"
    else:
        limiar_z = dist_mean + z_thresh * dist_std
        limiar_iqr = q3 + 2.5 * iqr
        limiar = (min(limiar_z, limiar_iqr) * 0.7) + (limite_urbano_km * 0.3)
        metodo_desc = (
            f"Híbrido adaptativo suavizado (z={limiar_z:.2f}, iqr={limiar_iqr:.2f}, "
            f"urbano={limite_urbano_km:.2f} → final={limiar:.2f} km)"
        )

    # =====================================================
    # 🧹 5️⃣ Detecção final
    # =====================================================
    flags = dist_min > limiar
    removidos = np.sum(flags)

    logger.info(
        f"🧹 Detecção de outliers [{metodo_desc}] | média={dist_mean:.2f} km | std={dist_std:.2f} | "
        f"densidade média (k5)={media_k5:.2f} km | limite dinâmico={limite_dinamico:.2f} km | "
        f"outliers detectados={removidos}/{len(pdvs)}"
    )

    return [(pdvs[i], bool(flags[i])) for i in range(len(pdvs))]


# ============================================================
# 🧠 Execução principal da clusterização
# ============================================================

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
    excluir_outliers: bool = False,
    z_thresh: float = 3.0,
) -> Dict[str, Any]:
    """
    Executa o fluxo completo de clusterização com detecção robusta de outliers.
    """

    logger.info(
        f"🏁 Iniciando clusterização | tenant_id={tenant_id} | {uf}-{cidade} "
        f"| algo={algo} | input_id={input_id} | clusterization_id={clusterization_id}"
    )

    # 1️⃣ Carrega PDVs
    pdvs = carregar_pdvs(tenant_id=tenant_id, input_id=input_id, uf=uf, cidade=cidade)
    if not pdvs:
        raise ValueError(
            f"Nenhum PDV encontrado para tenant_id={tenant_id}, input_id={input_id}, filtros={uf}-{cidade}."
        )
    logger.info(f"✅ {len(pdvs)} PDVs carregados (input_id={input_id}).")

    # 2️⃣ Detecta e salva outliers (modo híbrido)
    pdv_flags = detectar_outliers_geograficos(pdvs, z_thresh=z_thresh, metodo="hibrido")
    total_outliers = sum(1 for _, flag in pdv_flags if flag)

    try:
        salvar_outliers(tenant_id, clusterization_id, pdv_flags)
        logger.info(f"🗄️ {len(pdv_flags)} PDVs registrados com flag de outlier (total={total_outliers}).")
    except Exception as e:
        logger.warning(f"⚠️ Falha ao salvar tabela de outliers: {e}")

    # 3️⃣ Filtra se o usuário optou por excluir
    if excluir_outliers:
        pdvs_filtrados = [p for p, flag in pdv_flags if not flag]
        logger.info(f"📉 {total_outliers} outliers removidos | {len(pdvs_filtrados)} PDVs restantes.")
        pdvs = pdvs_filtrados
    else:
        logger.info("✅ Outliers incluídos (nenhum removido).")

    # 4️⃣ Snapshot de parâmetros
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

    # 5️⃣ Cria registro de execução
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
        # 6️⃣ Execução do algoritmo
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

        # 7️⃣ Persistência
        mapping = salvar_setores(tenant_id, run_id, setores)
        salvar_mapeamento_pdvs(tenant_id, run_id, mapping, labels, pdvs)
        logger.info(f"✅ Clusterização salva no banco (run_id={run_id}, clusterization_id={clusterization_id}).")

        # 8️⃣ Finaliza run
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
            "outliers": total_outliers,
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

#sales_router/src/sales_clusterization/application/cluster_use_case.py

# ============================================================
# 📦 src/sales_clusterization/application/cluster_use_case.py
# ============================================================

from typing import Optional, Dict, Any, List
from loguru import logger
import numpy as np
from sklearn.neighbors import NearestNeighbors
import math


from src.sales_clusterization.domain.sector_generator import kmeans_balanceado
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
from src.sales_clusterization.domain.operational_cluster_refiner import OperationalClusterRefiner


# ============================================================
# 🧠 Detecção de Outliers Geográficos (versão mais sensível)
# ============================================================
def detectar_outliers_geograficos(
    pdvs: List[PDV],
    z_thresh: float = 1.8,  # 🔹 antes 2.0 → mais sensível
    metodo: Optional[str] = None,
    limite_urbano_km: Optional[float] = None,
):
    if len(pdvs) < 5:
        logger.warning("⚠️ Poucos PDVs para detecção de outliers — nenhum removido.")
        return [(p, False) for p in pdvs]

    coords = np.array([[p.lat, p.lon] for p in pdvs])
    coords_rad = np.radians(coords)
    k = min(5, len(coords) - 1)
    nn = NearestNeighbors(n_neighbors=k + 1, metric="haversine")
    nn.fit(coords_rad)
    dist, _ = nn.kneighbors(coords_rad)

    dist_min = dist[:, 1] * 6371.0
    media_k5 = np.mean(dist[:, -1]) * 6371.0
    limite_dinamico = np.clip(media_k5 * 6, 3, 10)  # 🔹 antes 7× — menor = mais sensível

    if limite_urbano_km is None:
        limite_urbano_km = limite_dinamico

    dist_mean = np.mean(dist_min)
    dist_std = np.std(dist_min)
    q1, q3 = np.percentile(dist_min, [25, 75])
    iqr = q3 - q1

    # 🔹 Seleção adaptativa do método
    if metodo is None:
        if dist_std < 2:
            metodo = "iqr"
        elif dist_std > 5:
            metodo = "zscore"
        else:
            metodo = "hibrido"

    # 🔹 Ajuste dos limiares para mais sensibilidade
    if metodo == "iqr":
        limiar = q3 + 1.8 * iqr            # antes 2.5
    elif metodo == "zscore":
        limiar = dist_mean + z_thresh * 1.5 * dist_std  # antes z_thresh * std
    else:
        limiar_z = dist_mean + z_thresh * 1.5 * dist_std
        limiar_iqr = q3 + 1.8 * iqr
        limiar = (min(limiar_z, limiar_iqr) * 0.6) + (limite_urbano_km * 0.4)

    flags = dist_min > limiar
    removidos = np.sum(flags)
    logger.info(
        f"🧹 Outliers detectados={removidos}/{len(pdvs)} "
        f"| método={metodo} | limiar={limiar:.2f} km"
    )

    # 🚨 Alerta se dispersão acima do esperado
    if removidos / len(pdvs) > 0.05:
        logger.warning(
            f"🚨 {removidos} outliers ({removidos/len(pdvs):.1%}) — alta dispersão detectada."
        )

    return [(pdvs[i], bool(flags[i])) for i in range(len(pdvs))]



# ============================================================
# 🚀 Execução principal da clusterização
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
    z_thresh: float = 1.5,
    max_iter: int = 10,  # 🆕 Número máximo de iterações (parametrizável)
) -> Dict[str, Any]:
    """
    Executa o fluxo completo de clusterização com detecção robusta de outliers
    e refinamento operacional iterativo. 
    Agora com limite de iterações configurável (max_iter).
    """

    logger.info(
        f"🏁 Iniciando clusterização | tenant_id={tenant_id} | {uf}-{cidade} "
        f"| algo={algo} | input_id={input_id} | max_iter={max_iter}"
    )


    # ============================================================
    # 1️⃣ Carrega PDVs
    # ============================================================
    pdvs = carregar_pdvs(tenant_id=tenant_id, input_id=input_id, uf=uf, cidade=cidade)
    if not pdvs:
        raise ValueError(f"Nenhum PDV encontrado para tenant_id={tenant_id}, input_id={input_id}.")

    logger.info(f"✅ {len(pdvs)} PDVs carregados (input_id={input_id}).")

    # ============================================================
    # 2️⃣ Detecta e salva outliers
    # ============================================================
    pdv_flags = detectar_outliers_geograficos(pdvs, z_thresh=z_thresh, metodo="hibrido")
    total_outliers = sum(1 for _, flag in pdv_flags if flag)

    outliers_data = [
        {"pdv_id": getattr(p, "id", None), "lat": p.lat, "lon": p.lon, "is_outlier": bool(flag)}
        for p, flag in pdv_flags
    ]
    try:
        salvar_outliers(tenant_id, clusterization_id, outliers_data)
    except Exception as e:
        logger.warning(f"⚠️ Falha ao salvar outliers: {e}")

    if excluir_outliers:
        pdvs = [p for p, flag in pdv_flags if not flag]
        logger.info(f"📉 {total_outliers} outliers removidos | {len(pdvs)} PDVs restantes.")
    else:
        logger.info("✅ Outliers incluídos (nenhum removido).")

    # ============================================================
    # 3️⃣ Snapshot de parâmetros e criação de run
    # ============================================================
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
    logger.info(f"🆕 Execução registrada | run_id={run_id}")

    try:
        # ============================================================
        # 4️⃣ Instancia refinador operacional
        # ============================================================
        refiner = OperationalClusterRefiner(
            v_kmh=v_kmh,
            max_time_min=workday_min,
            max_dist_km=route_km_max,
            tempo_servico_min=service_min,
            max_iter=max_iter,
            tenant_id=tenant_id,  # 👈 adicionado
        )

         # ============================================================
        # 4️⃣-B NOVO MODO: KMEANS_SIMPLES (DEFAULT)
        # ============================================================
        if algo in ("kmeans_simples", "simples", None):
            logger.info("🧠 Modo simples: clusterização apenas por número máximo de PDVs (sem refino operacional).")

            total_pdvs = len(pdvs)
            k_inicial = max(1, math.ceil(total_pdvs / max_pdv_cluster))
            logger.info(f"📊 Total {total_pdvs} PDVs | Máx {max_pdv_cluster}/cluster → K inicial = {k_inicial}")

            # 🧮 Executa KMeans padrão (centros seguem densidade natural)
            setores_finais, labels = kmeans_setores(pdvs, k_inicial)

            # 🧩 Atualiza labels de cada PDV
            for i, p in enumerate(pdvs):
                p.cluster_label = int(labels[i]) if i < len(labels) else -1

            # 🗺️ Loga resumo por cluster
            for s in setores_finais:
                logger.debug(
                    f"📍 Cluster {s.cluster_label}: {s.n_pdvs} PDVs | "
                    f"Centro=({s.centro_lat:.5f}, {s.centro_lon:.5f}) | "
                    f"Raio med={s.raio_med_km:.2f} km | P95={s.raio_p95_km:.2f} km"
                )

            # 💾 Salva setores e mapeamento PDVs
            mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
            label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}
            for p in pdvs:
                if p.cluster_label in label_to_id:
                    p.cluster_id = label_to_id[p.cluster_label]

            salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)

            # ✅ Finaliza execução
            finalizar_run(run_id, status="done", k_final=k_inicial)
            logger.success(f"✅ Clusterização simples concluída | K={k_inicial} | run_id={run_id}")

            return {
                "tenant_id": tenant_id,
                "clusterization_id": clusterization_id,  # ✅ adicionada
                "run_id": run_id,
                "algo": algo,
                "k_final": k_inicial,
                "n_pdvs": len(pdvs),
                "outliers": total_outliers,
                "diagnostico": f"Clusterização simples concluída com K={k_inicial} e {len(pdvs)} PDVs."
            }




        # ============================================================
        # 5️⃣ KMEANS → clusterização operacional iterativa completa
        # ============================================================
        if algo == "kmeans":
            if k_forcado:
                k0 = k_forcado
                diag = {"modo": "forçado"}
                logger.info(f"📎 K forçado recebido: {k0}")
            else:
                k0, diag = estimar_k_inicial(
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

            
            logger.info("🧭 Executando KMeans balanceado com refinamento automático...")
            setores_finais = kmeans_balanceado(
                pdvs=pdvs,
                max_pdv_cluster=max_pdv_cluster,
                v_kmh=v_kmh,
                max_dist_km=route_km_max,
                max_time_min=workday_min,
                tempo_servico_min=service_min,
            )

            # ============================================================
            # 🚚 Geração de subrotas teóricas + reclusterização hierárquica
            # ============================================================
            logger.info("🚚 Gerando subrotas teóricas e avaliando limites operacionais...")
            setores_finais = refiner.gerar_subrotas_teoricas(
                pdvs=pdvs,
                setores_macro=setores_finais,
                dias_uteis=dias_uteis,
                freq=freq,
                max_pdv_cluster=max_pdv_cluster,
            )


          
            # 📊 Diagnóstico pós-refinamento — consolida tempos e distâncias das rotas teóricas
            tempos = [
                sc.get("tempo_min", 0)
                for s in setores_finais
                if getattr(s, "subclusters", None)
                for sc in (s.subclusters or [])
            ]

            distancias = [sc.get("dist_km", 0) for s in setores_finais for sc in getattr(s, "subclusters", [])]
            excedidos = [
                sc for s in setores_finais for sc in getattr(s, "subclusters", [])
                if sc.get("status") == "EXCEDIDO"
            ]

            tempo_medio_min = np.mean(tempos) if tempos else 0
            tempo_max_min = np.max(tempos) if tempos else 0
            distancia_media_km = np.mean(distancias) if distancias else 0
            dist_max_km = np.max(distancias) if distancias else 0

            diag["refinamento_operacional"] = {
                "clusters_excedidos": len(excedidos),
                "tempo_medio_min": round(float(tempo_medio_min), 2),
                "tempo_max_min": round(float(tempo_max_min), 2),
                "distancia_media_km": round(float(distancia_media_km), 2),
                "dist_max_km": round(float(dist_max_km), 2),
                "k_final": len(setores_finais),
                "dias_uteis": dias_uteis,
                "freq": freq,
                "subrotas_planejadas": max(1, int(dias_uteis / max(freq, 1))),
            }

            logger.info("📊 Diagnóstico consolidado (rotas teóricas):")
            logger.info(f"   - Clusters finais: {len(setores_finais)} | excedidos: {len(excedidos)}")
            logger.info(
                f"   - Tempo médio: {tempo_medio_min:.1f} min (máx {tempo_max_min:.1f}) | "
                f"Distância média: {distancia_media_km:.1f} km (máx {dist_max_km:.1f})"
            )


        # ============================================================
        # 6️⃣ DBSCAN híbrido balanceado (mantido)
        # ============================================================
        elif algo == "dbscan":
            logger.info("🔹 Executando DBSCAN balanceado...")
            setores, labels = dbscan_kmeans_balanceado(pdvs, max_pdv_cluster=max_pdv_cluster)
            for i, p in enumerate(pdvs):
                p.cluster_label = int(labels[i])
            setores_finais = refiner.subdividir_excedidos(setores, pdvs)
            avaliacoes = refiner.avaliar_clusters(setores_finais)
            diag = {"refinamento_operacional": {"clusters_excedidos": sum(r["status"] == "EXCEDIDO" for r in avaliacoes)}}

        # ============================================================
        # 7️⃣ Pipeline híbrido DBSCAN → KMeans → Subclusterização diária
        # ============================================================
        elif algo == "hibrido":
            logger.info("🧩 Executando pipeline híbrido DBSCAN → KMeans balanceado...")

            setores, labels = dbscan_kmeans_balanceado(
                pdvs=pdvs,
                max_pdv_cluster=max_pdv_cluster,
                frequencia_visita=freq,
                dias_uteis=dias_uteis,
                workday_min=workday_min,
                tempo_servico_min=service_min,
                v_kmh=v_kmh,
            )

            for i, p in enumerate(pdvs):
                p.cluster_label = int(labels[i])

            # ========================================================
            # 🚚 Subclusterização diária iterativa (rotas ≤ 600 min)
            # ========================================================
            logger.info("🚚 Iniciando subclusterização diária iterativa (rotas ≤ tempo máximo)...")

            setores_finais = refiner.refinar_com_subclusters_iterativo(
                pdvs=pdvs,
                dias_uteis=dias_uteis,
                freq=freq,
                max_pdv_cluster=max_pdv_cluster,
            )

            # ========================================================
            # 📊 Diagnóstico pós-refinamento (similar ao modo KMeans)
            # ========================================================
            tempos = [sc["tempo_min"] for s in setores_finais for sc in getattr(s, "subclusters", [])]
            distancias = [sc["dist_km"] for s in setores_finais for sc in getattr(s, "subclusters", [])]
            excedidos = [sc for s in setores_finais for sc in getattr(s, "subclusters", []) if sc["status"] == "EXCEDIDO"]

            tempo_medio_min = np.mean(tempos) if tempos else 0
            tempo_max_min = np.max(tempos) if tempos else 0
            distancia_media_km = np.mean(distancias) if distancias else 0
            dist_max_km = np.max(distancias) if distancias else 0

            diag = {
                "refinamento_operacional": {
                    "clusters_excedidos": len(excedidos),
                    "tempo_medio_min": round(float(tempo_medio_min), 2),
                    "tempo_max_min": round(float(tempo_max_min), 2),
                    "distancia_media_km": round(float(distancia_media_km), 2),
                    "dist_max_km": round(float(dist_max_km), 2),
                    "k_final": len(setores_finais),
                    "dias_uteis": dias_uteis,
                    "freq": freq,
                    "subrotas_planejadas": max(1, int(dias_uteis / max(freq, 1))),
                }
            }

            logger.info("📊 Diagnóstico híbrido pós-subclusterização:")
            logger.info(f"   - Clusters finais: {len(setores_finais)} | excedidos: {len(excedidos)}")
            logger.info(
                f"   - Tempo médio: {tempo_medio_min:.1f} min (máx {tempo_max_min:.1f}) | "
                f"Distância média: {distancia_media_km:.1f} km (máx {dist_max_km:.1f})"
            )



        # ============================================================
        # 7️⃣ Persistência final
        # ============================================================
        mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
        label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}
        for p in pdvs:
            if p.cluster_label in label_to_id:
                p.cluster_id = label_to_id[p.cluster_label]
        salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)

        k_final_exec = diag.get("refinamento_operacional", {}).get("k_final", len(setores_finais))
        finalizar_run(run_id, k_final=k_final_exec, status="done")
        logger.success(f"🏁 Clusterização concluída | run_id={run_id} | K={k_final_exec}")

        return {
            "tenant_id": tenant_id,
            "clusterization_id": clusterization_id,
            "run_id": run_id,
            "algo": algo,
            "k_final": k_final_exec,
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
                for s in setores_finais
            ],
        }

    except Exception as e:
        logger.error(f"❌ Erro durante clusterização (run_id={run_id}): {e}")
        finalizar_run(run_id, k_final=0, status="error", error=str(e))
        raise

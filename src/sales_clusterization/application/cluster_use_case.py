#sales_router/src/sales_clusterization/application/cluster_use_case.py

# ============================================================
# 📦 src/sales_clusterization/application/cluster_use_case.py
# ============================================================

from typing import Optional, Dict, Any, List
from loguru import logger
import numpy as np
from sklearn.neighbors import NearestNeighbors
import math
import numpy as np




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
from src.sales_clusterization.domain.balanced_geokmeans import balanced_geokmeans



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
        # 🧠 MODO KMEANS_SIMPLES — Balanced KMeans nativo (sem sklearn_extra)
        # ============================================================
        if algo == "kmeans_simples":
            from sklearn.cluster import KMeans
            import numpy as np
            from src.sales_clusterization.domain.k_estimator import _haversine_km
            from src.sales_clusterization.domain.entities import Setor

            logger.info("🧠 Modo simples balanceado: usando KMeans com redistribuição automática de clusters.")

            # --------------------------
            # 1️⃣ Preparação dos dados
            # --------------------------
            coords = np.array([[p.lat, p.lon] for p in pdvs])
            n_pdvs = len(coords)
            k_inicial = max(1, round(n_pdvs / max_pdv_cluster))
            logger.info(f"📊 Total {n_pdvs} PDVs | alvo ≈ {max_pdv_cluster}/cluster → K inicial ≈ {k_inicial}")

            # --------------------------
            # 2️⃣ Execução inicial do KMeans
            # --------------------------
            clf = KMeans(n_clusters=k_inicial, n_init=20, random_state=42)
            labels = clf.fit_predict(coords)
            centers = clf.cluster_centers_

            # ============================================================
            # 3️⃣ Redistribuição balanceada iterativa
            # ============================================================
            def balancear_clusters(coords, labels, centers, max_pdv_cluster, tolerancia=2, max_iter=10):
                labels = np.array(labels, dtype=int)
                for it in range(max_iter):
                    unique, counts = np.unique(labels, return_counts=True)
                    excesso = {u: c - max_pdv_cluster for u, c in zip(unique, counts) if c > max_pdv_cluster + tolerancia}
                    faltando = {u: max_pdv_cluster - c for u, c in zip(unique, counts) if c < max_pdv_cluster - tolerancia}

                    logger.debug(f"🔁 Iteração {it+1}: excesso={excesso} | faltando={faltando}")

                    if not excesso and not faltando:
                        break

                    for cid_excesso, qtd_excesso in excesso.items():
                        idx_excesso = np.where(labels == cid_excesso)[0]
                        if not len(idx_excesso):
                            continue

                        dist_to_centers = np.linalg.norm(coords[idx_excesso][:, None, :] - centers, axis=2)
                        ordenados = np.argsort(-dist_to_centers[:, cid_excesso])

                        for i in ordenados[:qtd_excesso]:
                            destinos_validos = [d for d, f in faltando.items() if f > 0]
                            if not destinos_validos:
                                break
                            dist_ao_destino = {d: dist_to_centers[i, d] for d in destinos_validos}
                            destino = min(dist_ao_destino, key=dist_ao_destino.get)
                            labels[idx_excesso[i]] = destino
                            faltando[destino] -= 1

                    for cid in np.unique(labels):
                        cluster_pts = coords[labels == cid]
                        if len(cluster_pts):
                            centers[cid] = cluster_pts.mean(axis=0)

                logger.success(f"⚖️ Rebalanceamento concluído após {it+1} iterações.")
                return labels, centers

            labels, centers = balancear_clusters(coords, labels, centers, max_pdv_cluster)

            # ============================================================
            # 4️⃣ Criação dos objetos Setor e persistência
            # ============================================================
            setores_finais = []
            for cid in np.unique(labels):
                cluster_pdvs = [p for p, lbl in zip(pdvs, labels) if lbl == cid]
                centro_lat = float(np.mean([p.lat for p in cluster_pdvs]))
                centro_lon = float(np.mean([p.lon for p in cluster_pdvs]))

                setor = Setor(
                    cluster_label=int(cid),
                    centro_lat=centro_lat,
                    centro_lon=centro_lon,
                    n_pdvs=len(cluster_pdvs),
                    raio_med_km=0,
                    raio_p95_km=0,
                )
                setores_finais.append(setor)
                logger.debug(f"📍 Cluster {cid}: {len(cluster_pdvs)} PDVs | Centro=({centro_lat:.5f}, {centro_lon:.5f})")

            for p, lbl in zip(pdvs, labels):
                p.cluster_label = int(lbl)

            mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
            label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}

            for p in pdvs:
                if hasattr(p, "cluster_label") and p.cluster_label in label_to_id:
                    p.cluster_id = label_to_id[p.cluster_label]

            salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)

            unique, counts = np.unique(labels, return_counts=True)
            media_cluster = np.mean(counts)
            desvio_cluster = np.std(counts)
            indice_equilibrio = round(max(counts) / max(1, min(counts)), 2)

            logger.info(
                f"📈 Diagnóstico: média={media_cluster:.1f} | desvio={desvio_cluster:.1f} "
                f"| índice={indice_equilibrio} | tamanhos={dict(zip(unique, counts))}"
            )

            finalizar_run(run_id, status="done", k_final=len(setores_finais))
            logger.success(f"✅ Clusterização simples balanceada concluída | K={len(setores_finais)} | run_id={run_id}")



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

            # ✅ Garante escopo local de np
            import numpy as np

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
        # 🧭 Novo modo: KMeans Geo Balanceado
        # ============================================================
        elif algo == "kmeans_geo":
            from src.sales_clusterization.domain.entities import Setor

            logger.info("🗺️  Modo Geo Balanceado: criando clusters espaciais coerentes com tamanho equilibrado.")
            k_inicial = max(1, round(len(pdvs) / max_pdv_cluster))
            logger.info(f"📊 Total {len(pdvs)} PDVs | alvo ≈ {max_pdv_cluster}/cluster → K inicial ≈ {k_inicial}")


            labels, centers = balanced_geokmeans(pdvs, k=k_inicial)
            setores_finais = []

            # Cria objetos Setor e calcula centroides
            for i in range(k_inicial):
                cluster_points = [p for p, lbl in zip(pdvs, labels) if lbl == i]
                centro_lat, centro_lon = centers[i]
                setor = Setor(
                    cluster_label=i,
                    centro_lat=centro_lat,
                    centro_lon=centro_lon,
                    n_pdvs=len(cluster_points),
                    raio_med_km=0,
                    raio_p95_km=0,
                )
                setores_finais.append(setor)
                logger.debug(f"📍 Cluster {i}: {len(cluster_points)} PDVs | Centro=({centro_lat:.5f}, {centro_lon:.5f})")

            # Atribui cluster_label e cluster_id a cada PDV
            for p, lbl in zip(pdvs, labels):
                p.cluster_label = int(lbl)

            mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
            label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}

            for p in pdvs:
                if hasattr(p, "cluster_label") and p.cluster_label in label_to_id:
                    p.cluster_id = label_to_id[p.cluster_label]

            salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)
            logger.success(f"✅ Clusterização Geo Balanceada concluída | K={k_inicial} | run_id={run_id}")

            return {
                "clusterization_id": clusterization_id,
                "run_id": run_id,
                "k_final": k_inicial,
                "n_pdvs": len(pdvs),
                "diagnostico": (
                    f"GeoKMeans balanceado concluído com K={k_inicial} clusters "
                    f"e {len(pdvs)} PDVs (diferença máxima ±1 PDV por cluster)."
                ),
            }

        # ============================================================
        # 🧭 Novo modo: Radial Geo Balanceado
        # ============================================================
        elif algo == "radial_geo":
            from src.sales_clusterization.domain.radial_balanced_clustering import radial_balanced_clustering
            from src.sales_clusterization.domain.entities import Setor

            logger.info("🌐 Modo Radial Geo: atribuição radial equilibrada em torno de centros geométricos.")
            k_inicial = max(1, round(len(pdvs) / max_pdv_cluster))
            logger.info(f"📊 Total {len(pdvs)} PDVs | alvo ≈ {max_pdv_cluster}/cluster → K inicial ≈ {k_inicial}")

            # ⚙️ Executa o algoritmo radial
            labels, centers = radial_balanced_clustering(pdvs, k=k_inicial, max_pdv_cluster=max_pdv_cluster)

            # ✅ Corrige: atribui o cluster_label antes de salvar
            for p, lbl in zip(pdvs, labels):
                p.cluster_label = int(lbl)

            # 🧩 Cria lista de setores
            setores_finais = []
            for i in range(k_inicial):
                cluster_points = [p for p in pdvs if p.cluster_label == i]
                if not cluster_points:
                    continue

                centro_lat, centro_lon = centers[i]
                setor = Setor(
                    cluster_label=i,
                    centro_lat=centro_lat,
                    centro_lon=centro_lon,
                    n_pdvs=len(cluster_points),
                    raio_med_km=0,
                    raio_p95_km=0,
                )
                setores_finais.append(setor)
                logger.debug(f"📍 Cluster {i}: {len(cluster_points)} PDVs | Centro=({centro_lat:.5f}, {centro_lon:.5f})")

            # 💾 Persistência
            mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
            label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}

            for p in pdvs:
                if hasattr(p, "cluster_label") and p.cluster_label in label_to_id:
                    p.cluster_id = label_to_id[p.cluster_label]

            salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)
            logger.success(f"✅ Clusterização Radial Geo concluída | K={k_inicial} | run_id={run_id}")

            return {
                "clusterization_id": clusterization_id,
                "run_id": run_id,
                "k_final": k_inicial,
                "n_pdvs": len(pdvs),
                "diagnostico": (
                    f"Radial Geo balanceado concluído com {k_inicial} clusters "
                    f"(diferença máxima ±1 PDV por cluster)."
                ),
            }

        # ============================================================
        # 🧭 Novo modo: Radial Geo Contínuo (com conectividade espacial)
        # ============================================================
        elif algo == "radial_geo_continuous":
            from src.sales_clusterization.domain.radial_geo_continuous import radial_geo_continuous
            from src.sales_clusterization.domain.entities import Setor

            logger.info("🌐 Modo Radial Geo Contínuo: priorizando continuidade espacial e equilíbrio.")
            k_inicial = max(1, round(len(pdvs) / max_pdv_cluster))
            logger.info(f"📊 Total {len(pdvs)} PDVs | alvo ≈ {max_pdv_cluster}/cluster → K inicial ≈ {k_inicial}")

            labels, centers = radial_geo_continuous(
                pdvs=pdvs,
                k=k_inicial,
                max_pdv_cluster=max_pdv_cluster,
                tolerancia=2,
                max_iter=10,
            )

            setores_finais = []
            for i in range(k_inicial):
                cluster_points = [p for p, lbl in zip(pdvs, labels) if lbl == i]
                if not cluster_points:
                    continue
                centro_lat, centro_lon = centers[i]
                setor = Setor(
                    cluster_label=i,
                    centro_lat=float(centro_lat),
                    centro_lon=float(centro_lon),
                    n_pdvs=len(cluster_points),
                    raio_med_km=0,
                    raio_p95_km=0,
                )
                setores_finais.append(setor)
                logger.debug(f"📍 Cluster {i}: {len(cluster_points)} PDVs | Centro=({centro_lat:.5f}, {centro_lon:.5f})")

            # Persistência
            mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
            label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}
            for p, lbl in zip(pdvs, labels):
                if hasattr(p, "cluster_label"):
                    p.cluster_id = label_to_id.get(lbl)
                    p.cluster_label = int(lbl)
            salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)

            logger.success(f"✅ Clusterização Radial Geo Contínua concluída | K={k_inicial} | run_id={run_id}")

            return {
                "clusterization_id": clusterization_id,
                "run_id": run_id,
                "k_final": k_inicial,
                "n_pdvs": len(pdvs),
                "diagnostico": (
                    f"Radial Geo Contínuo concluído com {k_inicial} clusters "
                    f"({len(pdvs)} PDVs, diferença máxima ±2 PDVs)."
                ),
            }
        
        # ============================================================
        # 🧭 Novo modo: Capacitated K-Means
        # ============================================================
        elif algo == "capacitated_kmeans":
            from src.sales_clusterization.domain.capacitated_kmeans import capacitated_kmeans
            from src.sales_clusterization.domain.entities import Setor
            import numpy as np  # ✅ necessário para diagnóstico estatístico


            logger.info("🚀 Modo Capacitated K-Means: balanceamento rápido com limite de PDVs por cluster.")
            diag = {"refinamento_operacional": {}}  # ✅ evita erro se diag não existir

            # ⚙️ Executa o algoritmo de clusterização
            labels, centers = capacitated_kmeans(pdvs, max_capacity=max_pdv_cluster)

            # 🧩 Criação dos objetos Setor
            setores_finais = []
            for i in range(len(centers)):
                cluster_points = [p for p, lbl in zip(pdvs, labels) if lbl == i]
                if not cluster_points:
                    continue
                centro_lat, centro_lon = centers[i]
                setor = Setor(
                    cluster_label=i,
                    centro_lat=float(centro_lat),
                    centro_lon=float(centro_lon),
                    n_pdvs=len(cluster_points),
                    raio_med_km=0,
                    raio_p95_km=0,
                )
                setores_finais.append(setor)
                logger.debug(f"📍 Cluster {i}: {len(cluster_points)} PDVs | Centro=({centro_lat:.5f}, {centro_lon:.5f})")

            # 💾 Persistência dos resultados
            mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
            label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}

            for p, lbl in zip(pdvs, labels):
                if hasattr(p, "cluster_label"):
                    p.cluster_label = int(lbl)
                    p.cluster_id = label_to_id.get(lbl)

            salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)
            logger.success(f"✅ Capacitated K-Means concluído | K={len(setores_finais)} | run_id={run_id}")

            # 🧠 Diagnóstico simples
            diag["refinamento_operacional"]["k_final"] = len(setores_finais)
            diag["refinamento_operacional"]["media_pdv_cluster"] = round(
                np.mean([s.n_pdvs for s in setores_finais]), 2
            )
            diag["refinamento_operacional"]["max_pdv_cluster"] = max([s.n_pdvs for s in setores_finais])
            diag["refinamento_operacional"]["min_pdv_cluster"] = min([s.n_pdvs for s in setores_finais])

        # ============================================================
        # 🧭 Novo modo: Capacitated Sweep Clustering
        # ============================================================
        elif algo == "capacitated_sweep":
            from src.sales_clusterization.domain.capacitated_sweep import capacitated_sweep
            from src.sales_clusterization.domain.entities import Setor
            import numpy as np

            logger.info("🚀 Modo Capacitated Sweep: setorização contínua e linear com limite de PDVs por cluster.")
            diag = {"refinamento_operacional": {}}

            # ⚙️ Executa o algoritmo
            labels, centers = capacitated_sweep(pdvs, max_capacity=max_pdv_cluster)

            # 🧩 Criação dos objetos Setor
            setores_finais = []
            for i in range(len(centers)):
                cluster_points = [p for p, lbl in zip(pdvs, labels) if lbl == i]
                if not cluster_points:
                    continue

                centro_lat, centro_lon = centers[i]
                setor = Setor(
                    cluster_label=i,
                    centro_lat=float(centro_lat),
                    centro_lon=float(centro_lon),
                    n_pdvs=len(cluster_points),
                    raio_med_km=0,
                    raio_p95_km=0,
                )
                setores_finais.append(setor)
                logger.debug(
                    f"📍 Cluster {i}: {len(cluster_points)} PDVs | Centro=({centro_lat:.5f}, {centro_lon:.5f})"
                )

            # 💾 Persistência dos resultados
            mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
            label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}

            for p, lbl in zip(pdvs, labels):
                if hasattr(p, "cluster_label"):
                    p.cluster_label = int(lbl)
                    p.cluster_id = label_to_id.get(lbl)

            salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)
            logger.success(f"✅ Capacitated Sweep concluído | K={len(setores_finais)} | run_id={run_id}")

            # 🧠 Diagnóstico simples
            diag["refinamento_operacional"]["k_final"] = len(setores_finais)
            diag["refinamento_operacional"]["media_pdv_cluster"] = round(
                np.mean([s.n_pdvs for s in setores_finais]), 2
            )
            diag["refinamento_operacional"]["max_pdv_cluster"] = max([s.n_pdvs for s in setores_finais])
            diag["refinamento_operacional"]["min_pdv_cluster"] = min([s.n_pdvs for s in setores_finais])


        # 7️⃣ Persistência final (evita duplicação)
        # Evita regravar clusters já persistidos em modos que fazem isso internamente
        if algo not in ("capacitated_kmeans", "capacitated_sweep"):
            mapping_cluster_id = salvar_setores(tenant_id, run_id, setores_finais)
            label_to_id = {s.cluster_label: mapping_cluster_id.get(s.cluster_label) for s in setores_finais}
            for p in pdvs:
                if p.cluster_label in label_to_id:
                    p.cluster_id = label_to_id[p.cluster_label]
            salvar_mapeamento_pdvs(tenant_id, run_id, pdvs)

        # ============================================================
        # 🏁 Finalização do run
        # ============================================================
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
    
    

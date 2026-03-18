    # sales_router/src/sales_routing/application/balanced_subcluster_splitter.py

    import math
    from typing import List, Dict, Any
    import numpy as np
    from sklearn.cluster import KMeans
    from loguru import logger

    from src.sales_routing.domain.entities.cluster_data_entity import ClusterData, PDVData
    from src.sales_routing.application.route_optimizer import RouteOptimizer


    def _coord_array(pdvs: List[PDVData]) -> np.ndarray:
        return np.array([[p.lat, p.lon] for p in pdvs], dtype=float)


    def _centroid(pdvs: List[PDVData]) -> tuple[float, float]:
        coords = _coord_array(pdvs)
        return float(coords[:, 0].mean()), float(coords[:, 1].mean())


    def _euclidean_sq(a: tuple[float, float], b: tuple[float, float]) -> float:
        return (a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2


    def _build_group_dict(
        group_id: int,
        pdvs_group: List[PDVData],
        cluster: ClusterData,
        optimizer: RouteOptimizer,
        aplicar_two_opt: bool = False,
    ) -> Dict[str, Any]:
        centro = {"lat": cluster.centro_lat, "lon": cluster.centro_lon}
        pdvs_dict = [{"pdv_id": p.pdv_id, "lat": p.lat, "lon": p.lon} for p in pdvs_group]

        rota_result = optimizer.calcular_rota(
            centro=centro,
            pdvs=pdvs_dict,
            aplicar_two_opt=aplicar_two_opt,
        )

        return {
            "subcluster_id": group_id,
            "n_pdvs": len(pdvs_group),
            "tempo_total_min": rota_result["tempo_total_min"],
            "dist_total_km": rota_result["distancia_total_km"],
            "pdvs": rota_result["sequencia"],
            "rota_coord": rota_result["rota_coord"],
            "centro_lat": cluster.centro_lat,
            "centro_lon": cluster.centro_lon,
        }


    def _split_large_group(
        pdvs_group: List[PDVData],
        max_pdvs_rota: int,
    ) -> List[List[PDVData]]:
        """
        Divide grupos acima do limite máximo usando KMeans local.
        """
        if len(pdvs_group) <= max_pdvs_rota:
            return [pdvs_group]

        k_local = math.ceil(len(pdvs_group) / max_pdvs_rota)
        k_local = min(k_local, len(pdvs_group))

        coords = _coord_array(pdvs_group)
        kmeans = KMeans(n_clusters=k_local, random_state=42, n_init="auto").fit(coords)
        labels = kmeans.labels_

        result = []
        for sub_id in range(k_local):
            bloco = [p for i, p in enumerate(pdvs_group) if labels[i] == sub_id]
            if bloco:
                result.append(bloco)

        return result


    def _merge_small_groups(
        groups: List[List[PDVData]],
        min_pdvs_rota: int,
        max_pdvs_rota: int,
    ) -> List[List[PDVData]]:
        """
        Junta grupos pequenos ao grupo mais próximo, respeitando max_pdvs_rota quando possível.
        """
        if not groups:
            return groups

        changed = True

        while changed:
            changed = False

            small_indexes = [i for i, g in enumerate(groups) if len(g) < min_pdvs_rota]
            if not small_indexes:
                break

            for idx in small_indexes:
                if idx >= len(groups):
                    continue

                grupo_pequeno = groups[idx]
                if len(grupo_pequeno) >= min_pdvs_rota:
                    continue

                centroid_small = _centroid(grupo_pequeno)

                candidatos = []
                for j, grupo_destino in enumerate(groups):
                    if j == idx:
                        continue

                    novo_tamanho = len(grupo_pequeno) + len(grupo_destino)
                    penalidade = 0 if novo_tamanho <= max_pdvs_rota else 1000000

                    centroid_dest = _centroid(grupo_destino)
                    dist = _euclidean_sq(centroid_small, centroid_dest) + penalidade
                    candidatos.append((dist, j))

                if not candidatos:
                    continue

                candidatos.sort(key=lambda x: x[0])
                best_j = candidatos[0][1]

                groups[best_j].extend(grupo_pequeno)
                del groups[idx]
                changed = True
                break

        return groups


    def _rebalance_overflow(
        groups: List[List[PDVData]],
        min_pdvs_rota: int,
        max_pdvs_rota: int,
    ) -> List[List[PDVData]]:
        """
        Segunda passada:
        se ainda houver grupo acima do máximo, tenta redistribuir PDVs para grupos abaixo do máximo,
        priorizando proximidade ao centroide do grupo destino.
        """
        if len(groups) <= 1:
            return groups

        changed = True
        while changed:
            changed = False

            for i, group in enumerate(groups):
                if len(group) <= max_pdvs_rota:
                    continue

                centroid_i = _centroid(group)

                destinos = []
                for j, gdest in enumerate(groups):
                    if i == j:
                        continue
                    if len(gdest) >= max_pdvs_rota:
                        continue
                    destinos.append((j, _centroid(gdest)))

                if not destinos:
                    continue

                excesso = len(group) - max_pdvs_rota

                for _ in range(excesso):
                    if len(group) <= max_pdvs_rota:
                        break

                    melhor_p = None
                    melhor_j = None
                    melhor_score = None

                    for p in group:
                        p_coord = (float(p.lat), float(p.lon))
                        for j, c_dest in destinos:
                            if len(groups[j]) >= max_pdvs_rota:
                                continue
                            score = _euclidean_sq(p_coord, c_dest)
                            if melhor_score is None or score < melhor_score:
                                melhor_score = score
                                melhor_p = p
                                melhor_j = j

                    if melhor_p is None or melhor_j is None:
                        break

                    group.remove(melhor_p)
                    groups[melhor_j].append(melhor_p)
                    changed = True

        groups = _merge_small_groups(groups, min_pdvs_rota, max_pdvs_rota)
        return groups


    def dividir_cluster_em_subclusters_balanceados(
        cluster: ClusterData,
        pdvs_cluster: List[PDVData],
        dias_uteis: int,
        freq_padrao: float,
        v_kmh: float,
        service_min: int,
        alpha_path: float = 1.3,
        aplicar_two_opt: bool = False,
        min_pdvs_rota: int = 8,
        max_pdvs_rota: int = 12,
        modo_calculo: str = "frequencia",
    ) -> Dict[str, Any]:
        """
        Estratégia balanceada:
        1) Define K inicial
        2) KMeans inicial
        3) Split de grupos grandes
        4) Merge de grupos pequenos
        5) Rebalanceamento fino
        6) Calcula rota real com RouteOptimizer
        """

        if not pdvs_cluster:
            return {
                "cluster_id": cluster.cluster_id,
                "n_subclusters": 0,
                "tempo_total_mes": 0.0,
                "dist_total_mes": 0.0,
                "mean_pdvs": 0.0,
                "subclusters": [],
            }

        if min_pdvs_rota <= 0:
            raise ValueError("min_pdvs_rota deve ser > 0")

        if max_pdvs_rota < min_pdvs_rota:
            raise ValueError("max_pdvs_rota deve ser >= min_pdvs_rota")

        for p in pdvs_cluster:
            p.freq_visita = getattr(p, "freq_visita", freq_padrao)

        total_pdvs = len(pdvs_cluster)
        visitas_totais = sum(float(p.freq_visita) for p in pdvs_cluster)

        if modo_calculo == "frequencia":
            freq = max(1, int(freq_padrao or 1))
            k_inicial = max(1, dias_uteis // freq)
            logger.info(
                f"📦 Cluster {cluster.cluster_id}: modo balanceado/frequencia | "
                f"dias_uteis={dias_uteis} | freq={freq} | k_inicial={k_inicial}"
            )
        elif modo_calculo == "proporcional":
            k_inicial = max(1, math.ceil(visitas_totais / dias_uteis))
            logger.info(
                f"📦 Cluster {cluster.cluster_id}: modo balanceado/proporcional | "
                f"visitas_totais={visitas_totais:.1f} | dias_uteis={dias_uteis} | k_inicial={k_inicial}"
            )
        else:
            k_inicial = max(1, math.ceil(total_pdvs / max_pdvs_rota))
            logger.info(
                f"📦 Cluster {cluster.cluster_id}: modo balanceado/capacidade | "
                f"PDVs={total_pdvs} | max_pdvs_rota={max_pdvs_rota} | k_inicial={k_inicial}"
            )

        k_inicial = min(k_inicial, total_pdvs)

        coords = _coord_array(pdvs_cluster)
        kmeans = KMeans(n_clusters=k_inicial, random_state=42, n_init="auto").fit(coords)
        labels = kmeans.labels_

        groups = []
        for sub_id in range(k_inicial):
            grupo = [p for i, p in enumerate(pdvs_cluster) if labels[i] == sub_id]
            if grupo:
                groups.append(grupo)

        logger.info(
            f"🧩 Cluster {cluster.cluster_id}: após KMeans inicial -> "
            f"{[len(g) for g in groups]}"
        )

        split_groups = []
        for g in groups:
            split_groups.extend(_split_large_group(g, max_pdvs_rota=max_pdvs_rota))

        logger.info(
            f"✂️ Cluster {cluster.cluster_id}: após split grandes -> "
            f"{[len(g) for g in split_groups]}"
        )

        merged_groups = _merge_small_groups(
            groups=split_groups,
            min_pdvs_rota=min_pdvs_rota,
            max_pdvs_rota=max_pdvs_rota,
        )

        logger.info(
            f"🔗 Cluster {cluster.cluster_id}: após merge pequenos -> "
            f"{[len(g) for g in merged_groups]}"
        )

        final_groups = _rebalance_overflow(
            groups=merged_groups,
            min_pdvs_rota=min_pdvs_rota,
            max_pdvs_rota=max_pdvs_rota,
        )

        logger.info(
            f"⚖️ Cluster {cluster.cluster_id}: após rebalance -> "
            f"{[len(g) for g in final_groups]}"
        )

        optimizer = RouteOptimizer(
            v_kmh=v_kmh,
            service_min=service_min,
            alpha_path=alpha_path,
        )

        subclusters = []
        for idx, grupo in enumerate(final_groups, start=1):
            subclusters.append(
                _build_group_dict(
                    group_id=idx,
                    pdvs_group=grupo,
                    cluster=cluster,
                    optimizer=optimizer,
                    aplicar_two_opt=aplicar_two_opt,
                )
            )

        tempo_total_mes = sum(s["tempo_total_min"] for s in subclusters)
        dist_total_mes = sum(s["dist_total_km"] for s in subclusters)
        pdvs_medio = np.mean([s["n_pdvs"] for s in subclusters]) if subclusters else 0

        logger.success(
            f"✅ Cluster {cluster.cluster_id}: balanceado concluído | "
            f"{len(subclusters)} rotas | tempo_total_mes={tempo_total_mes:.1f} min | "
            f"dist_total_mes={dist_total_mes:.1f} km | mean_pdvs={pdvs_medio:.1f}"
        )

        return {
            "cluster_id": cluster.cluster_id,
            "n_subclusters": len(subclusters),
            "tempo_total_mes": tempo_total_mes,
            "dist_total_mes": dist_total_mes,
            "mean_pdvs": round(float(pdvs_medio), 1),
            "subclusters": subclusters,
        }


    def gerar_subclusters_balanceados(
        clusters: List[ClusterData],
        pdvs: List[PDVData],
        dias_uteis: int,
        freq_padrao: float,
        v_kmh: float,
        service_min: int,
        alpha_path: float = 1.3,
        aplicar_two_opt: bool = False,
        min_pdvs_rota: int = 8,
        max_pdvs_rota: int = 12,
        modo_calculo: str = "frequencia",
    ) -> List[Dict[str, Any]]:
        resultados = []

        logger.info("🚀 Iniciando subclusterização balanceada...")

        for cluster in clusters:
            pdvs_cluster = [p for p in pdvs if p.cluster_id == cluster.cluster_id]
            if not pdvs_cluster:
                logger.warning(f"⚠️ Cluster {cluster.cluster_id} sem PDVs — ignorado.")
                continue

            logger.info(f"\n🧭 Processando Cluster {cluster.cluster_id} ({len(pdvs_cluster)} PDVs)")
            resultado = dividir_cluster_em_subclusters_balanceados(
                cluster=cluster,
                pdvs_cluster=pdvs_cluster,
                dias_uteis=dias_uteis,
                freq_padrao=freq_padrao,
                v_kmh=v_kmh,
                service_min=service_min,
                alpha_path=alpha_path,
                aplicar_two_opt=aplicar_two_opt,
                min_pdvs_rota=min_pdvs_rota,
                max_pdvs_rota=max_pdvs_rota,
                modo_calculo=modo_calculo,
            )
            resultados.append(resultado)

        logger.success(f"🏁 Subclusterização balanceada concluída para {len(resultados)} clusters.")
        return resultados
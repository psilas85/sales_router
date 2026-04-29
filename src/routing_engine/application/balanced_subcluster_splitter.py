#sales_router/src/routing_engine/application/balanced_subcluster_splitter.py

from __future__ import annotations

import math
from typing import List, Dict, Any

import numpy as np
from loguru import logger
from sklearn.cluster import KMeans

from routing_engine.domain.entities import PDVData, RouteGroup
from routing_engine.domain.utils_geo import mean_center
from routing_engine.application.route_optimizer import RouteOptimizer


def _coord_array(pdvs: List[PDVData]) -> np.ndarray:
    return np.array([[p.lat, p.lon] for p in pdvs], dtype=float)


def _centroid(pdvs: List[PDVData]) -> tuple[float, float]:
    return mean_center([(p.lat, p.lon) for p in pdvs])


def _euclidean_sq(a: tuple[float, float], b: tuple[float, float]) -> float:
    return (a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2


def _split_large_group(
    pdvs_group: List[PDVData],
    max_pdvs_rota: int,
) -> List[List[PDVData]]:
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
    if len(groups) <= 1:
        return groups

    changed = True
    while changed:
        changed = False

        for i, group in enumerate(groups):
            if len(group) <= max_pdvs_rota:
                continue

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


def dividir_grupo_em_rotas_balanceadas(
    route_group: RouteGroup,
    dias_uteis: int,
    freq_padrao: float,
    route_optimizer: RouteOptimizer,
    aplicar_two_opt: bool = False,
    min_pdvs_rota: int = 8,
    max_pdvs_rota: int = 12,
    modo_calculo: str = "frequencia",
) -> Dict[str, Any]:
    pdvs_group = route_group.pdvs

    if not pdvs_group:
        return {
            "grupo_utilizado": route_group.group_id,
            "fonte_grupo": route_group.group_type,
            "n_subclusters": 0,
            "tempo_total_mes": 0.0,
            "dist_total_mes": 0.0,
            "mean_pdvs": 0.0,
            "subclusters": [],
        }

    for p in pdvs_group:
        p.freq_visita = getattr(p, "freq_visita", freq_padrao)

    total_pdvs = len(pdvs_group)
    visitas_totais = sum(float(p.freq_visita) for p in pdvs_group)

    if modo_calculo == "frequencia":
        freq = max(1, int(freq_padrao or 1))
        k_inicial = max(1, dias_uteis // freq)
        logger.info(
            f"📦 Grupo={route_group.group_id} | tipo={route_group.group_type} | "
            f"modo=freq | PDVs={total_pdvs} | dias_uteis={dias_uteis} | freq={freq} | "
            f"k_inicial={k_inicial}"
        )
    elif modo_calculo == "proporcional":
        k_inicial = max(1, math.ceil(visitas_totais / dias_uteis))
        logger.info(
            f"📦 Grupo={route_group.group_id} | tipo={route_group.group_type} | "
            f"modo=proporcional | visitas_totais={visitas_totais:.1f} | dias_uteis={dias_uteis} | "
            f"k_inicial={k_inicial}"
        )
    else:
        k_inicial = max(1, math.ceil(total_pdvs / max_pdvs_rota))
        logger.info(
            f"📦 Grupo={route_group.group_id} | tipo={route_group.group_type} | "
            f"modo=capacidade | PDVs={total_pdvs} | max_pdvs_rota={max_pdvs_rota} | "
            f"k_inicial={k_inicial}"
        )

    k_inicial = min(k_inicial, total_pdvs)

    coords = _coord_array(pdvs_group)
    kmeans = KMeans(n_clusters=k_inicial, random_state=42, n_init="auto").fit(coords)
    labels = kmeans.labels_

    groups = []
    for sub_id in range(k_inicial):
        grupo = [p for i, p in enumerate(pdvs_group) if labels[i] == sub_id]
        if grupo:
            groups.append(grupo)

    split_groups = []
    for g in groups:
        split_groups.extend(_split_large_group(g, max_pdvs_rota=max_pdvs_rota))

    merged_groups = _merge_small_groups(
        groups=split_groups,
        min_pdvs_rota=min_pdvs_rota,
        max_pdvs_rota=max_pdvs_rota,
    )

    final_groups = _rebalance_overflow(
        groups=merged_groups,
        min_pdvs_rota=min_pdvs_rota,
        max_pdvs_rota=max_pdvs_rota,
    )

    subclusters = []
    for idx, grupo in enumerate(final_groups, start=1):
        centro = {
            "lat": route_group.centro_lat,
            "lon": route_group.centro_lon,
        }

        pdvs_dict = [
            {
                "pdv_id": p.pdv_id,
                "cnpj": p.cnpj,
                "nome_fantasia": p.nome_fantasia,
                "endereco_completo": p.endereco_completo,
                "logradouro": p.logradouro,
                "numero": p.numero,
                "bairro": p.bairro,
                "cidade": p.cidade,
                "uf": p.uf,
                "cep": p.cep,
                "grupo_utilizado": p.grupo_utilizado,
                "fonte_grupo": p.fonte_grupo,
                "lat": p.lat,
                "lon": p.lon,
            }
            for p in grupo
        ]

        rota_result = route_optimizer.calcular_rota(
            centro=centro,
            pdvs=pdvs_dict,
            aplicar_two_opt=aplicar_two_opt,
        )

        subclusters.append(
            {
                "subcluster_id": idx,
                "grupo_utilizado": route_group.group_id,
                "fonte_grupo": route_group.group_type,
                "n_pdvs": len(grupo),
                "tempo_total_min": rota_result["tempo_total_min"],
                "dist_total_km": rota_result["distancia_total_km"],
                "pdvs": rota_result["sequencia"],
                "rota_coord": rota_result["rota_coord"],
                "centro_lat": route_group.centro_lat,
                "centro_lon": route_group.centro_lon,
            }
        )

    tempo_total_mes = sum(s["tempo_total_min"] for s in subclusters)
    dist_total_mes = sum(s["dist_total_km"] for s in subclusters)
    pdvs_medio = np.mean([s["n_pdvs"] for s in subclusters]) if subclusters else 0

    logger.success(
        f"✅ Grupo {route_group.group_id}: {len(subclusters)} rotas | "
        f"tempo_total_mes={tempo_total_mes:.1f} min | "
        f"dist_total_mes={dist_total_mes:.1f} km | mean_pdvs={pdvs_medio:.1f}"
    )

    return {
        "grupo_utilizado": route_group.group_id,
        "fonte_grupo": route_group.group_type,
        "n_subclusters": len(subclusters),
        "tempo_total_mes": float(tempo_total_mes),
        "dist_total_mes": float(dist_total_mes),
        "mean_pdvs": round(float(pdvs_medio), 1),
        "subclusters": subclusters,
    }
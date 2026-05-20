# ============================================================
# 📦 src/sales_clusterization/domain/banda_rebalancer.py
# ============================================================
"""
Pós-processador OPCIONAL: aplica uma banda [mínimo, máximo] de PDVs por
setor sobre o resultado de qualquer um dos 3 modos do KMeans da Simulação
Inteligente (operacional / capacidade / fixo).

Regras (definidas com o usuário):
- Só atua quando o usuário envia `limite_min` e/ou `limite_max`. Sem eles,
  o caller nem chama esta função — a clusterização segue intacta.
- Modos "operacional" e "capacidade" → REESTRUTURAM de verdade:
    * setor acima do máximo  → subdividido (KMeans) até caber no teto;
    * setor abaixo do mínimo → DISSOLVIDO: cada PDV é reatribuído ao setor
      de centroide mais próximo com folga (atribuição tipo Voronoi). NÃO
      cria ilhas — cada PDV vai pro setor em cujo território ele cai, em
      vez de o grupo pequeno inteiro ser jogado num único vizinho (o que
      poderia "abraçar" um terceiro setor e deixá-lo encravado). PDV sem
      destino com folga segue no grupo de origem, marcado `ABAIXO_MIN`.
- Modo "fixo" → NÃO reestrutura (preserva o K travado pelo usuário):
  apenas avalia e marca `banda_status` ("OK" / "FORA_DA_BANDA") + loga aviso.

Observação: nos 3 modos do KMeans cada `Setor` chega aqui com `.pdvs`
preenchido (kmeans_fixo, kmeans_balanceado e kmeans_setores anexam a lista).
"""

import math
from typing import List, Optional, Tuple

import numpy as np
from loguru import logger
from sklearn.cluster import KMeans

from .entities import PDV, Setor
from .k_estimator import _haversine_km
from .sector_generator import _raios_cluster, kmeans_setores

RANDOM_STATE = 42


# ============================================================
# 🔹 Auxiliares
# ============================================================
def _centro(pdvs: List[PDV]) -> Tuple[float, float]:
    """Centroide (média lat/lon) de um grupo de PDVs."""
    lat = float(np.mean([p.lat for p in pdvs]))
    lon = float(np.mean([p.lon for p in pdvs]))
    return lat, lon


def _subdividir(grupo: List[PDV], limite_max: int) -> List[List[PDV]]:
    """Divide um grupo de PDVs em subgrupos, todos <= limite_max."""
    if len(grupo) <= limite_max:
        return [grupo]

    k = math.ceil(len(grupo) / limite_max)
    coords = np.array([[p.lat, p.lon] for p in grupo])

    partes: List[List[PDV]] = []
    try:
        labels = KMeans(
            n_clusters=k, random_state=RANDOM_STATE, n_init=10
        ).fit_predict(coords)
        for cid in range(k):
            sub = [p for p, lbl in zip(grupo, labels) if lbl == cid]
            if sub:
                partes.append(sub)
    except Exception as e:  # pragma: no cover - defensivo
        logger.warning(f"⚠️ KMeans falhou na subdivisão da banda: {e}")
        partes = []

    # Fallback raríssimo: KMeans não separou (ex.: pontos coincidentes).
    # Ordena no espaço antes de fatiar pra que os pedaços fiquem ao menos
    # contíguos (faixas), não aleatórios. Garante terminação e teto.
    if len(partes) <= 1:
        ordenado = sorted(grupo, key=lambda p: (p.lat, p.lon))
        return [
            ordenado[i:i + limite_max]
            for i in range(0, len(ordenado), limite_max)
        ]

    # Recursão nos subgrupos que ainda passem do teto (cada parte é
    # estritamente menor que `grupo`, então a recursão termina).
    resultado: List[List[PDV]] = []
    for sub in partes:
        if len(sub) > limite_max:
            resultado.extend(_subdividir(sub, limite_max))
        else:
            resultado.append(sub)
    return resultado


def _fundir_pequenos(
    grupos: List[List[PDV]],
    limite_min: int,
    limite_max: Optional[int],
) -> List[List[PDV]]:
    """
    Dissolve grupos abaixo do mínimo redistribuindo cada PDV ao grupo de
    centroide mais próximo com folga (atribuição tipo Voronoi).

    Por que PDV a PDV, e não o grupo inteiro: jogar o grupo pequeno todo
    num único vizinho pode fazer o setor resultante "abraçar" um terceiro
    setor que esteja no meio do caminho — criando uma ilha encravada.
    Reatribuindo ponto a ponto, cada PDV vai pro setor em cujo território
    geográfico ele cai, então o resultado continua espacialmente coerente.

    Respeita o teto: se o destino mais próximo está cheio, tenta o
    próximo. PDV sem nenhum destino com folga segue no grupo de origem,
    que é mantido e marcado ABAIXO_MIN pelo caller.
    """
    grupos = [list(g) for g in grupos if g]
    travados: set = set()  # grupos pequenos que não têm pra onde escoar

    while True:
        vivos = [i for i, g in enumerate(grupos) if g]
        if len(vivos) <= 1:
            break
        pequenos = [
            i for i in vivos
            if len(grupos[i]) < limite_min and i not in travados
        ]
        if not pequenos:
            break

        # Dissolve o menor grupo primeiro.
        idx = min(pequenos, key=lambda i: len(grupos[i]))
        a_dissolver = list(grupos[idx])
        destinos = [i for i in vivos if i != idx]
        # Centroides dos destinos — calculados uma vez por dissolução
        # (o drift ao absorver poucos PDVs é desprezível).
        centros = {i: _centro(grupos[i]) for i in destinos}

        sobra: List[PDV] = []
        for p in a_dissolver:
            # Destinos ordenados pela distância do PDV ao centroide; o
            # primeiro com folga é o "território" natural daquele PDV.
            ordenados = sorted(
                destinos,
                key=lambda i: _haversine_km((p.lat, p.lon), centros[i]),
            )
            alvo = next(
                (
                    i for i in ordenados
                    if limite_max is None or len(grupos[i]) < limite_max
                ),
                None,
            )
            if alvo is None:
                sobra.append(p)
            else:
                grupos[alvo].append(p)

        if len(sobra) == len(a_dissolver):
            # Nada escoou (todos os destinos no teto) → trava o grupo.
            travados.add(idx)
        else:
            # O que não coube segue como grupo e é reavaliado na próxima
            # volta; se travar lá, o caller marca ABAIXO_MIN.
            grupos[idx] = sobra

    return [g for g in grupos if g]


def _regenerar_subclusters(setor: Setor, refiner, dias_uteis: int, freq: int) -> None:
    """
    Recalcula as rotas diárias (subclusters) de um setor — necessário no
    modo "operacional" quando a banda dividiu/fundiu setores, senão os
    subclusters persistidos ficariam defasados. Espelha a lógica interna
    de OperationalClusterRefiner.refinar_com_subclusters_iterativo.
    """
    pdvs_setor = setor.pdvs or []
    if not pdvs_setor:
        setor.subclusters = []
        return

    n_sub = max(1, int(dias_uteis / max(freq, 1)))
    n_sub = min(n_sub, len(pdvs_setor))
    sub_setores, _ = kmeans_setores(pdvs_setor, n_sub)

    setor.subclusters = []
    for j, sub in enumerate(sub_setores):
        coords_sub = [(p.lat, p.lon) for p in (sub.pdvs or []) if p.lat and p.lon]
        if not coords_sub:
            continue
        dist_km, tempo_min, rota = refiner.calcular_rota_simulada(
            coords_sub, (sub.centro_lat, sub.centro_lon)
        )
        excedeu = tempo_min > refiner.max_time_min or dist_km > refiner.max_dist_km
        setor.subclusters.append({
            "seq": j + 1,
            "centro_lat": sub.centro_lat,
            "centro_lon": sub.centro_lon,
            "n_pdvs": len(coords_sub),
            "dist_km": dist_km,
            "tempo_min": tempo_min,
            "status": "EXCEDIDO" if excedeu else "OK",
            "rota_sequencia": rota,
        })


def _setor_de_grupo(label: int, grupo: List[PDV]) -> Setor:
    """Reconstrói uma entidade Setor a partir de um grupo de PDVs."""
    centro = _centro(grupo)
    pts = [(p.lat, p.lon) for p in grupo]
    med, p95 = _raios_cluster(centro, pts)
    for p in grupo:
        p.cluster_label = label
    return Setor(
        cluster_label=label,
        centro_lat=centro[0],
        centro_lon=centro[1],
        n_pdvs=len(grupo),
        raio_med_km=float(med),
        raio_p95_km=float(p95),
        pdvs=grupo,
        coords=pts,
        metrics={"raio_med_km": float(med), "raio_p95_km": float(p95)},
    )


# ============================================================
# 🎚️ Função principal
# ============================================================
def rebalancear_para_banda(
    setores: List[Setor],
    *,
    limite_min: Optional[int],
    limite_max: Optional[int],
    modo: str,
    refiner=None,
    dias_uteis: Optional[int] = None,
    freq: Optional[int] = None,
) -> List[Setor]:
    """
    Aplica a banda [limite_min, limite_max] de PDVs por setor.

    Args:
        setores: setores produzidos pelo modo do KMeans (com `.pdvs`).
        limite_min: piso de PDVs por setor (None = sem piso).
        limite_max: teto de PDVs por setor (None = sem teto).
        modo: "operacional" | "capacidade" | "fixo".
        refiner: OperationalClusterRefiner — usado só no modo "operacional"
            para regenerar os subclusters dos setores alterados.
        dias_uteis, freq: necessários para regenerar subclusters (operacional).

    Returns:
        Lista de setores — reestruturada (operacional/capacidade) ou a
        original anotada com `banda_status` (fixo).
    """
    if not setores:
        return setores
    if limite_min is None and limite_max is None:
        return setores

    # Validação da banda — falha cedo com mensagem clara.
    if limite_max is not None and limite_max < 1:
        raise ValueError("O máximo de PDVs por setor deve ser >= 1.")
    if limite_min is not None and limite_min < 1:
        raise ValueError("O mínimo de PDVs por setor deve ser >= 1.")
    if (
        limite_min is not None
        and limite_max is not None
        and limite_min > limite_max
    ):
        raise ValueError(
            f"Banda inválida: mínimo ({limite_min}) é maior que o "
            f"máximo ({limite_max}) de PDVs por setor."
        )

    grupos = [list(s.pdvs) for s in setores if getattr(s, "pdvs", None)]
    if not grupos:
        logger.warning(
            "⚠️ Banda ignorada: setores sem PDVs anexados (nada a reestruturar)."
        )
        return setores

    # ------------------------------------------------------------
    # 🔒 Modo "fixo": K travado pelo usuário — só avalia, não reestrutura.
    # ------------------------------------------------------------
    if modo == "fixo":
        for s in setores:
            n = s.n_pdvs
            violacoes = []
            if limite_max is not None and n > limite_max:
                violacoes.append(f"acima do máx ({limite_max})")
            if limite_min is not None and n < limite_min:
                violacoes.append(f"abaixo do mín ({limite_min})")
            s.metrics = dict(s.metrics or {})
            if violacoes:
                s.metrics["banda_status"] = "FORA_DA_BANDA"
                logger.warning(
                    f"⚠️ Modo fixo | setor {s.cluster_label} com {n} PDVs "
                    f"{' e '.join(violacoes)} — K travado, não reestruturado."
                )
            else:
                s.metrics["banda_status"] = "OK"
        return setores

    # ------------------------------------------------------------
    # 🔧 Modos "operacional" e "capacidade": reestruturam.
    # ------------------------------------------------------------
    n_antes = len(grupos)

    if limite_max is not None:
        divididos: List[List[PDV]] = []
        for g in grupos:
            divididos.extend(_subdividir(g, limite_max))
        grupos = divididos

    if limite_min is not None:
        grupos = _fundir_pequenos(grupos, limite_min, limite_max)

    setores_novos: List[Setor] = []
    for label, grupo in enumerate(grupos):
        s = _setor_de_grupo(label, grupo)
        if limite_min is not None and s.n_pdvs < limite_min:
            s.metrics["banda_status"] = "ABAIXO_MIN"
            logger.warning(
                f"⚠️ Setor {label} ficou com {s.n_pdvs} PDVs (< mín "
                f"{limite_min}) — sem destino de fusão dentro do teto."
            )
        else:
            s.metrics["banda_status"] = "OK"

        if modo == "operacional" and refiner is not None and dias_uteis:
            _regenerar_subclusters(s, refiner, dias_uteis, freq or 1)

        setores_novos.append(s)

    logger.info(
        f"🎚️ Banda aplicada | modo={modo} | mín={limite_min} máx={limite_max} "
        f"| setores {n_antes} → {len(setores_novos)} "
        f"| tamanhos={[s.n_pdvs for s in setores_novos]}"
    )
    return setores_novos

# sales_router/src/sales_clusterization/domain/consultor_nearest.py
#
# Setorização por consultor (Execução Operacional): cada consultor
# cadastrado é o centro de um setor.
#
# Sem banda min/máx → cada PDV vai ao consultor mais próximo (Haversine).
#
# Com banda min e/ou máx preenchida → atribuição capacitada ÓTIMA: um
# problema de transporte resolvido por min-cost flow (OR-Tools). Minimiza
# a soma das distâncias PDV→consultor respeitando a banda por consultor.
# Isso elimina as "ilhas" da antiga atribuição gulosa, que era dependente
# da ordem de leitura: quando um consultor enchia, os PDVs seguintes eram
# empurrados para consultores distantes.
#
# Banda inviável é relaxada e sinalizada — a setorização SEMPRE conclui
# ("sinalizar e concluir"):
#   - n_consultores * min > total de PDVs → piso relaxado;
#   - n_consultores * max < total de PDVs → teto relaxado.
# Setores fora da banda recebem `banda_status` (EXCEDENTE / ABAIXO_MIN).
#
# Portado e adaptado de clusterization_engine/domain/consultor_nearest.py
# para o modelo de entidades (PDV/Setor) do sales_clusterization.

from typing import Optional

from loguru import logger
from ortools.graph.python import min_cost_flow

from src.sales_clusterization.domain.entities import PDV, Setor
from src.sales_clusterization.domain.haversine_utils import haversine

# Custo do min-cost flow é inteiro — distância (km) é escalada p/ metros.
_CUSTO_ESCALA = 1000


def _p95(valores: list[float]) -> float:
    if not valores:
        return 0.0
    ordenados = sorted(valores)
    pos = max(0, min(len(ordenados) - 1, round(0.95 * (len(ordenados) - 1))))
    return ordenados[pos]


def _atribuir_otimo(
    dist: list[list[float]],
    n_cons: int,
    m: int,
    min_pdv: Optional[int],
    max_pdv: Optional[int],
) -> tuple[list[int], bool, bool]:
    """Atribuição capacitada ÓTIMA via min-cost flow (problema de
    transporte): minimiza a soma das distâncias PDV→consultor respeitando
    a banda [min, max] por consultor.

    Rede de fluxo:
        fonte ──(cap 1)──▶ PDV_i ──(cap 1, custo=dist)──▶ consultor_j
        consultor_j ──(cap hi-lo)──▶ sorvedouro
    O piso `lo` por consultor entra pela transformação clássica de limite
    inferior em arco (ajuste de oferta/demanda nos nós).

    Retorna (atrib, min_relaxado, max_relaxado), onde atrib[i] = índice do
    consultor do PDV i.
    """
    lo = min_pdv if (min_pdv and min_pdv > 0) else 0
    hi = max_pdv if (max_pdv and max_pdv > 0) else m

    min_relaxado = False
    max_relaxado = False

    # Piso inviável: não há PDVs suficientes p/ todo consultor bater o mín.
    if lo and lo * n_cons > m:
        logger.warning(
            f"⚠️ Piso de {lo} PDVs × {n_cons} consultores > {m} PDVs — "
            f"mínimo relaxado (alguns setores ficarão abaixo)."
        )
        lo = 0
        min_relaxado = True

    # Teto inviável: os tetos somados não comportam todos os PDVs.
    if hi * n_cons < m:
        logger.warning(
            f"⚠️ Teto de {hi} PDVs × {n_cons} consultores < {m} PDVs — "
            f"máximo relaxado (alguns setores ficarão acima)."
        )
        hi = m
        max_relaxado = True

    smcf = min_cost_flow.SimpleMinCostFlow()

    # Nós: 0 = fonte | 1..m = PDVs | m+1..m+n = consultores | m+n+1 = sorvedouro
    fonte = 0
    sorvedouro = 1 + m + n_cons

    def no_pdv(i: int) -> int:
        return 1 + i

    def no_cons(j: int) -> int:
        return 1 + m + j

    for i in range(m):
        smcf.add_arc_with_capacity_and_unit_cost(fonte, no_pdv(i), 1, 0)
        for j in range(n_cons):
            smcf.add_arc_with_capacity_and_unit_cost(
                no_pdv(i),
                no_cons(j),
                1,
                int(round(dist[i][j] * _CUSTO_ESCALA)),
            )
    for j in range(n_cons):
        smcf.add_arc_with_capacity_and_unit_cost(
            no_cons(j), sorvedouro, max(0, hi - lo), 0
        )

    # Ofertas/demandas: a fonte injeta m unidades; o piso `lo` de cada
    # consultor é forçado puxando `lo` da demanda do consultor.
    smcf.set_node_supply(fonte, m)
    smcf.set_node_supply(sorvedouro, -(m - lo * n_cons))
    for j in range(n_cons):
        smcf.set_node_supply(no_cons(j), -lo)

    status = smcf.solve()
    if status != smcf.OPTIMAL:
        logger.warning(
            f"⚠️ min-cost flow status={status} — fallback p/ proximidade pura."
        )
        atrib = [min(range(n_cons), key=lambda j: dist[i][j]) for i in range(m)]
        return atrib, min_relaxado, max_relaxado

    atrib: list[int] = [-1] * m
    for arc in range(smcf.num_arcs()):
        if smcf.flow(arc) <= 0:
            continue
        tail = smcf.tail(arc)
        head = smcf.head(arc)
        # Só interessam os arcos PDV→consultor com fluxo.
        if 1 <= tail <= m and (1 + m) <= head <= (m + n_cons):
            atrib[tail - 1] = head - 1 - m

    # Defensivo: PDV sem arco saturado (não deveria ocorrer) → mais próximo.
    for i in range(m):
        if atrib[i] < 0:
            atrib[i] = min(range(n_cons), key=lambda j: dist[i][j])

    return atrib, min_relaxado, max_relaxado


def clusterizar_consultor_nearest(
    pdvs: list[PDV],
    consultores: list[dict],
    max_pdv: Optional[int] = None,
    min_pdv: Optional[int] = None,
) -> list[Setor]:
    """Atribui cada PDV a um consultor cadastrado.

    consultores: list de dict com chaves `id`, `nome`, `lat`, `lon`.
    Sem banda → proximidade pura. Com banda → atribuição capacitada ótima.
    Retorna a lista de Setor (um por consultor); marca `pdv.cluster_label`.
    """
    if not consultores:
        raise ValueError("Nenhum consultor selecionado para a setorização.")
    if max_pdv is not None and min_pdv is not None and min_pdv > max_pdv:
        raise ValueError("O mínimo de PDVs não pode ser maior que o máximo.")

    n_cons = len(consultores)
    m = len(pdvs)

    # dist[i][j] = distância do PDV i ao consultor j.
    dist: list[list[float]] = []
    for pdv in pdvs:
        dist.append(
            [
                haversine((pdv.lat, pdv.lon), (c["lat"], c["lon"]))
                for c in consultores
            ]
        )

    usar_banda = max_pdv is not None or min_pdv is not None
    min_relaxado = False
    max_relaxado = False

    if not usar_banda:
        # Proximidade pura — já é o ótimo sem restrição de capacidade.
        atrib = [min(range(n_cons), key=lambda j: dist[i][j]) for i in range(m)]
    else:
        atrib, min_relaxado, max_relaxado = _atribuir_otimo(
            dist, n_cons, m, min_pdv, max_pdv
        )

    # ── Monta os setores ──────────────────────────────────────────
    setores: list[Setor] = []
    for j, c in enumerate(consultores):
        idxs = [i for i in range(m) if atrib[i] == j]
        for i in idxs:
            pdvs[i].cluster_label = j

        dists_setor = [dist[i][j] for i in idxs]
        raio_med = (
            round(sum(dists_setor) / len(dists_setor), 3) if dists_setor else 0.0
        )
        raio_p95 = round(_p95(dists_setor), 3)

        banda = "OK"
        if max_pdv is not None and len(idxs) > max_pdv:
            banda = "EXCEDENTE"
        elif min_pdv is not None and len(idxs) < min_pdv:
            banda = "ABAIXO_MIN"

        excedentes = (
            max(0, len(idxs) - max_pdv) if max_pdv is not None else 0
        )

        setores.append(
            Setor(
                cluster_label=j,
                centro_lat=c["lat"],
                centro_lon=c["lon"],
                n_pdvs=len(idxs),
                raio_med_km=raio_med,
                raio_p95_km=raio_p95,
                metrics={
                    "banda_status": banda,
                    "excedentes": excedentes,
                },
                pdvs=[pdvs[i] for i in idxs],
                consultor_id=str(c["id"]),
                consultor_nome=c.get("nome"),
            )
        )

    logger.info(
        f"🧭 consultor_nearest | {m} PDVs → {n_cons} setores | "
        f"banda min={min_pdv} máx={max_pdv} | "
        f"tamanhos={[s.n_pdvs for s in setores]}"
        + (" | piso relaxado" if min_relaxado else "")
        + (" | teto relaxado" if max_relaxado else "")
    )
    return setores

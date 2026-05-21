# sales_router/src/sales_clusterization/application/setorizacao_manual.py
#
# Pipeline de SETORIZAÇÃO MANUAL da Execução Operacional.
#
# Difere da setorização operacional automática (consultor_nearest):
#  - NÃO roda algoritmo — recebe a atribuição PDV→consultor já pronta,
#    montada pelo usuário no editor de "laço" (lasso) do mapa;
#  - é sempre derivada de uma setorização existente (a "origem"), de quem
#    herda o carregamento (input_id), a UF e a cidade;
#  - persiste como uma NOVA setorização (novo cluster_run, algo="manual"),
#    sem sobrescrever a de origem.
#
# Reaproveita os helpers de persistência de setorizacao_operacional.py.

from uuid import uuid4

from loguru import logger

from src.database.db_connection import get_connection
from src.sales_clusterization.domain.entities import PDV, Setor
from src.sales_clusterization.domain.haversine_utils import haversine
from src.sales_clusterization.application.setorizacao_operacional import (
    _criar_run,
    _finalizar_run,
    _salvar_mapeamento,
    _salvar_setores,
    _set_operacional,
    carregar_consultores,
)

_ALGO = "manual"


def _p95(valores: list[float]) -> float:
    if not valores:
        return 0.0
    ordenados = sorted(valores)
    pos = max(0, min(len(ordenados) - 1, round(0.95 * (len(ordenados) - 1))))
    return ordenados[pos]


# ============================================================
# Leitura da setorização de origem
# ============================================================
def _buscar_origem(tenant_id: int, origem_clusterization_id: str) -> dict:
    """run_id / input_id / uf / cidade do cluster_run de origem."""
    with get_connection() as conn:
        _set_operacional(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, input_id, uf, cidade
                FROM cluster_run
                WHERE tenant_id = %s AND clusterization_id = %s
                ORDER BY criado_em DESC
                LIMIT 1;
                """,
                (tenant_id, origem_clusterization_id),
            )
            row = cur.fetchone()
    if not row:
        raise ValueError("Setorização de origem não encontrada.")
    return {
        "run_id": int(row[0]),
        "input_id": str(row[1]) if row[1] is not None else None,
        "uf": row[2],
        "cidade": row[3],
    }


def _pdv_ids_da_origem(tenant_id: int, run_id: int) -> set[int]:
    """Conjunto de pdv_id que pertencem à setorização de origem — usado
    para validar que o editor só atribui PDVs daquele carregamento."""
    with get_connection() as conn:
        _set_operacional(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT DISTINCT pdv_id
                FROM cluster_setor_pdv
                WHERE tenant_id = %s AND run_id = %s;
                """,
                (tenant_id, int(run_id)),
            )
            return {int(r[0]) for r in cur.fetchall() if r[0] is not None}


def _carregar_pdvs_por_id(tenant_id: int, pdv_ids: list[int]) -> dict[int, PDV]:
    """PDVs de operacional.pdvs indexados por id (apenas geocodificados)."""
    ids = sorted({int(i) for i in pdv_ids})
    if not ids:
        return {}
    with get_connection() as conn:
        _set_operacional(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, cnpj, cidade, uf, pdv_lat, pdv_lon
                FROM pdvs
                WHERE tenant_id = %s
                  AND id = ANY(%s)
                  AND pdv_lat IS NOT NULL
                  AND pdv_lon IS NOT NULL;
                """,
                (tenant_id, ids),
            )
            rows = cur.fetchall()
    out: dict[int, PDV] = {}
    for r in rows:
        out[int(r[0])] = PDV(
            id=int(r[0]),
            cnpj=r[1],
            nome=None,
            cidade=r[2],
            uf=r[3],
            lat=float(r[4]),
            lon=float(r[5]),
        )
    return out


# ============================================================
# Orquestração — setorização manual síncrona
# ============================================================
def salvar_setorizacao_manual(
    *,
    tenant_id: int,
    origem_clusterization_id: str,
    descricao: str,
    atribuicoes: list,
) -> dict:
    """Persiste uma atribuição PDV→consultor montada manualmente como uma
    NOVA setorização operacional.

    `atribuicoes`: [{"consultor_id": str, "pdv_ids": [int, ...]}, ...]

    Retorna {clusterization_id, run_id, status, n_setores, n_pdvs}.
    """
    if not atribuicoes:
        raise ValueError("Nenhuma atribuição informada.")

    origem = _buscar_origem(tenant_id, origem_clusterization_id)
    if not origem["input_id"]:
        raise ValueError(
            "Setorização de origem sem carregamento associado."
        )

    pdv_ids_validos = _pdv_ids_da_origem(tenant_id, origem["run_id"])

    # ── Normaliza + valida as atribuições ──────────────────────────
    grupos: list[tuple[str, list[int]]] = []
    consultor_ids: list[str] = []
    todos_pdv_ids: list[int] = []
    for a in atribuicoes:
        a = a or {}
        cid = str(a.get("consultor_id") or "").strip()
        if not cid:
            raise ValueError("Atribuição sem consultor.")
        try:
            pids = [int(x) for x in (a.get("pdv_ids") or [])]
        except (TypeError, ValueError):
            raise ValueError("Lista de PDVs inválida na atribuição.")
        if not pids:
            raise ValueError("Há um consultor sem nenhum PDV no laço.")
        for pid in pids:
            if pid not in pdv_ids_validos:
                raise ValueError(
                    f"PDV {pid} não pertence à setorização de origem."
                )
        if cid in consultor_ids:
            raise ValueError(
                "O mesmo consultor aparece em mais de uma atribuição."
            )
        consultor_ids.append(cid)
        grupos.append((cid, pids))
        todos_pdv_ids.extend(pids)

    # Cada PDV em um único consultor.
    if len(todos_pdv_ids) != len(set(todos_pdv_ids)):
        raise ValueError("Há PDVs atribuídos a mais de um consultor.")

    # ── Resolve consultores e PDVs ─────────────────────────────────
    consultores = carregar_consultores(tenant_id, consultor_ids)
    cons_por_id = {c["id"]: c for c in consultores}
    faltam_cons = [c for c in consultor_ids if c not in cons_por_id]
    if faltam_cons:
        raise ValueError(
            "Consultor(es) não encontrado(s), inativo(s) ou sem "
            "geolocalização."
        )

    pdvs_por_id = _carregar_pdvs_por_id(tenant_id, todos_pdv_ids)
    faltam_pdv = [p for p in todos_pdv_ids if p not in pdvs_por_id]
    if faltam_pdv:
        raise ValueError(
            f"{len(faltam_pdv)} PDV(s) sem geolocalização — não podem ser "
            "atribuídos manualmente."
        )

    # ── Monta setores e PDVs no modelo de domínio ──────────────────
    setores: list[Setor] = []
    pdvs_persistir: list[PDV] = []
    for label, (cid, pids) in enumerate(grupos):
        cons = cons_por_id[cid]
        pdvs_setor = [pdvs_por_id[p] for p in pids]
        raios = [
            haversine((cons["lat"], cons["lon"]), (pv.lat, pv.lon))
            for pv in pdvs_setor
        ]
        setor = Setor(
            cluster_label=label,
            centro_lat=float(cons["lat"]),
            centro_lon=float(cons["lon"]),
            n_pdvs=len(pdvs_setor),
            raio_med_km=(sum(raios) / len(raios)) if raios else 0.0,
            raio_p95_km=_p95(raios),
        )
        setor.consultor_id = cid
        setor.consultor_nome = cons["nome"]
        setor.pdvs = pdvs_setor
        setores.append(setor)
        pdvs_persistir.extend(pdvs_setor)

    # ── Persiste como nova setorização ─────────────────────────────
    params_snapshot = {
        "algo": _ALGO,
        "manual": True,
        "origem_clusterization_id": str(origem_clusterization_id),
        "n_consultores": len(setores),
        "consultor_ids": consultor_ids,
    }
    clusterization_id = str(uuid4())
    run_id = _criar_run(
        tenant_id, origem["uf"], origem["cidade"], params_snapshot,
        descricao, origem["input_id"], clusterization_id, algo=_ALGO,
    )

    try:
        mapping = _salvar_setores(tenant_id, run_id, setores)
        for s in setores:
            cluster_id = mapping[int(s.cluster_label)]
            for pdv in (s.pdvs or []):
                pdv.cluster_id = cluster_id
        n_pdvs = _salvar_mapeamento(tenant_id, run_id, pdvs_persistir)
        _finalizar_run(run_id, k_final=len(setores), status="done")
        logger.info(
            f"[SETORIZACAO_MANUAL] run={run_id} "
            f"clusterization_id={clusterization_id} "
            f"origem={origem_clusterization_id} setores={len(setores)} "
            f"pdvs={n_pdvs}"
        )
        return {
            "clusterization_id": clusterization_id,
            "run_id": run_id,
            "status": "done",
            "n_setores": len(setores),
            "n_pdvs": n_pdvs,
        }
    except Exception as e:
        _finalizar_run(run_id, k_final=0, status="error", error=str(e))
        logger.error(
            f"[SETORIZACAO_MANUAL][ERRO] run={run_id}: {e}", exc_info=True
        )
        raise

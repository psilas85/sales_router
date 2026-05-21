# sales_router/src/sales_clusterization/application/setorizacao_operacional.py
#
# Pipeline de SETORIZAÇÃO da Execução Operacional.
#
# Difere da Simulação:
#  - lê os PDVs de um carregamento operacional (operacional.pdvs);
#  - usa consultores cadastrados (public.consultores) como centros fixos;
#  - roda o algoritmo `consultor_nearest` (síncrono — sem job RQ);
#  - persiste em operacional.cluster_run/cluster_setor/cluster_setor_pdv.
#
# Tabelas-globais (consultores) seguem em `public`, resolvidas via search_path.

import json
from uuid import uuid4

from loguru import logger

from src.database.db_connection import get_connection
from src.sales_clusterization.domain.consultor_nearest import (
    clusterizar_consultor_nearest,
)
from src.sales_clusterization.infrastructure.operacional_cluster_schema import (
    ensure_operacional_cluster_schema,
)
from src.sales_clusterization.infrastructure.persistence.database_reader import (
    carregar_pdvs,
)

_ALGO = "consultor_nearest"


def _uf_to_str(uf) -> str | None:
    """Normaliza UF para CSV ('MG' ou 'MG,SP')."""
    if uf is None:
        return None
    if isinstance(uf, (list, tuple)):
        parts = [str(u).strip().upper() for u in uf if str(u).strip()]
        return ",".join(parts) if parts else None
    return str(uf).strip().upper() or None


def _cidade_to_str(cidade) -> str | None:
    """Normaliza cidade(s) para texto ('São Paulo' ou 'São Paulo, Campinas').
    Diferente de UF, preserva o caixa original (nome de exibição)."""
    if cidade is None:
        return None
    if isinstance(cidade, (list, tuple)):
        parts = [str(c).strip() for c in cidade if str(c).strip()]
        return ", ".join(parts) if parts else None
    return str(cidade).strip() or None


# ============================================================
# Conexão com search_path no schema operacional
# ============================================================
def _set_operacional(conn) -> None:
    with conn.cursor() as cur:
        cur.execute("SET search_path TO operacional, public")


# ============================================================
# Leitura de consultores selecionados (public.consultores)
# ============================================================
def carregar_consultores(tenant_id: int, consultor_ids: list) -> list[dict]:
    """Consultores ativos e geocodificados, no formato esperado pelo
    algoritmo: dict com id / nome / lat / lon."""
    ids = [str(i).strip() for i in (consultor_ids or []) if str(i).strip()]
    if not ids:
        return []
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT id, consultor, lat, lon
                FROM consultores
                WHERE tenant_id = %s
                  AND id = ANY(%s::uuid[])
                  AND ativo = TRUE
                  AND lat IS NOT NULL
                  AND lon IS NOT NULL
                """,
                (tenant_id, ids),
            )
            rows = cur.fetchall()
    return [
        {"id": str(r[0]), "nome": r[1], "lat": float(r[2]), "lon": float(r[3])}
        for r in rows
    ]


# ============================================================
# Persistência (operacional.cluster_*)
# ============================================================
def _criar_run(tenant_id, uf, cidade, params, descricao, input_id,
               clusterization_id, algo: str = _ALGO) -> int:
    with get_connection() as conn:
        ensure_operacional_cluster_schema(conn)
        _set_operacional(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cluster_run (
                    tenant_id, clusterization_id, descricao, input_id,
                    uf, cidade, algo, params, status, criado_em
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,'running',NOW())
                RETURNING id;
                """,
                (
                    tenant_id, clusterization_id, descricao, input_id,
                    _uf_to_str(uf), _cidade_to_str(cidade), algo,
                    json.dumps(params, ensure_ascii=False),
                ),
            )
            run_id = cur.fetchone()[0]
        conn.commit()
    return run_id


def _finalizar_run(run_id, k_final, status="done", error=None) -> None:
    with get_connection() as conn:
        _set_operacional(conn)
        with conn.cursor() as cur:
            cur.execute(
                """
                UPDATE cluster_run
                SET finished_at = NOW(), k_final = %s, status = %s, error = %s
                WHERE id = %s;
                """,
                (int(k_final), status, error, int(run_id)),
            )
        conn.commit()


def _salvar_setores(tenant_id, run_id, setores) -> dict:
    """Grava cluster_setor (com consultor_id/nome). Retorna {label: id}."""
    mapping = {}
    with get_connection() as conn:
        _set_operacional(conn)
        with conn.cursor() as cur:
            for s in setores:
                metrics = dict(getattr(s, "metrics", None) or {})
                metrics.setdefault("raio_med_km", float(s.raio_med_km))
                metrics.setdefault("raio_p95_km", float(s.raio_p95_km))
                cur.execute(
                    """
                    INSERT INTO cluster_setor (
                        tenant_id, run_id, cluster_label, nome,
                        centro_lat, centro_lon, n_pdvs, metrics,
                        tempo_medio_min, tempo_max_min,
                        distancia_media_km, dist_max_km, subclusters,
                        consultor_id, consultor_nome
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,0,0,0,0,'[]',%s,%s)
                    RETURNING id;
                    """,
                    (
                        int(tenant_id), int(run_id), int(s.cluster_label),
                        s.consultor_nome or f"Setor {s.cluster_label}",
                        float(s.centro_lat), float(s.centro_lon),
                        int(s.n_pdvs),
                        json.dumps(metrics, ensure_ascii=False),
                        s.consultor_id, s.consultor_nome,
                    ),
                )
                mapping[int(s.cluster_label)] = cur.fetchone()[0]
        conn.commit()
    return mapping


def _salvar_mapeamento(tenant_id, run_id, pdvs) -> int:
    sql = """
        INSERT INTO cluster_setor_pdv
            (tenant_id, run_id, cluster_id, pdv_id, lat, lon, cidade, uf, cnpj)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s);
    """
    total = 0
    with get_connection() as conn:
        _set_operacional(conn)
        with conn.cursor() as cur:
            for p in pdvs:
                if getattr(p, "cluster_id", None) is None:
                    continue
                cur.execute(
                    sql,
                    (
                        int(tenant_id), int(run_id), int(p.cluster_id),
                        int(p.id), float(p.lat), float(p.lon),
                        p.cidade, p.uf,
                        (p.cnpj or "").strip() if p.cnpj else None,
                    ),
                )
                total += 1
        conn.commit()
    return total


# ============================================================
# Orquestração — setorização operacional síncrona
# ============================================================
def executar_setorizacao_operacional(
    *,
    tenant_id: int,
    input_id: str,
    uf,
    cidade: str | None,
    descricao: str,
    consultor_ids: list,
    max_pdv: int | None = None,
    min_pdv: int | None = None,
    params: dict | None = None,
) -> dict:
    """Roda a setorização operacional de ponta a ponta (síncrona).
    Retorna {clusterization_id, run_id, status, n_setores, n_pdvs}."""
    pdvs = carregar_pdvs(tenant_id, input_id, uf, cidade, schema="operacional")
    if not pdvs:
        raise ValueError(
            "Nenhum PDV no carregamento para a UF/cidade informadas."
        )

    consultores = carregar_consultores(tenant_id, consultor_ids)
    if not consultores:
        raise ValueError(
            "Nenhum consultor ativo e geocodificado entre os selecionados."
        )

    params_snapshot = dict(params or {})
    params_snapshot.update({
        "algo": _ALGO,
        "max_pdv_cluster": max_pdv,
        "min_pdv_cluster": min_pdv,
        "consultor_ids": [c["id"] for c in consultores],
        "n_consultores": len(consultores),
    })

    clusterization_id = str(uuid4())
    run_id = _criar_run(
        tenant_id, uf, cidade, params_snapshot, descricao,
        input_id, clusterization_id,
    )

    try:
        setores = clusterizar_consultor_nearest(
            pdvs, consultores, max_pdv=max_pdv, min_pdv=min_pdv
        )
        mapping = _salvar_setores(tenant_id, run_id, setores)
        for s in setores:
            cluster_id = mapping[int(s.cluster_label)]
            for pdv in (s.pdvs or []):
                pdv.cluster_id = cluster_id
        _salvar_mapeamento(tenant_id, run_id, pdvs)
        _finalizar_run(run_id, k_final=len(setores), status="done")
        logger.info(
            f"[SETORIZACAO_OPERACIONAL] run={run_id} "
            f"clusterization_id={clusterization_id} setores={len(setores)} "
            f"pdvs={len(pdvs)}"
        )
        return {
            "clusterization_id": clusterization_id,
            "run_id": run_id,
            "status": "done",
            "n_setores": len(setores),
            "n_pdvs": len(pdvs),
        }
    except Exception as e:
        _finalizar_run(run_id, k_final=0, status="error", error=str(e))
        logger.error(
            f"[SETORIZACAO_OPERACIONAL][ERRO] run={run_id}: {e}", exc_info=True
        )
        raise

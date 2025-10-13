#sales_clusterization/infrastructure/persistence/database_writer.py

import json
from typing import List, Dict
from src.sales_clusterization.domain.entities import Setor, PDV
from src.database.db_connection import get_connection


def criar_run(uf: str | None, cidade: str | None, algo: str, params: dict) -> int:
    """
    Cria um registro de execução na tabela cluster_run e retorna o run_id.
    """
    sql = """
        INSERT INTO cluster_run (uf, cidade, algo, params, status)
        VALUES (%s, %s, %s, %s, 'running')
        RETURNING id;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (uf, cidade, algo, json.dumps(params, ensure_ascii=False)))
            run_id = cur.fetchone()[0]
            conn.commit()
    return run_id


def finalizar_run(run_id: int, k_final: int, status: str = "done", error: str | None = None):
    """
    Atualiza o status de uma execução.
    """
    sql = """
        UPDATE cluster_run
        SET finished_at = NOW(), k_final = %s, status = %s, error = %s
        WHERE id = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (k_final, status, error, run_id))
            conn.commit()


def salvar_setores(run_id: int, setores: List[Setor]) -> Dict[int, int]:
    """
    Insere os setores (macroclusters) e retorna o mapping cluster_label -> cluster_setor.id
    """
    mapping = {}
    sql = """
        INSERT INTO cluster_setor
            (run_id, cluster_label, nome, centro_lat, centro_lon, n_pdvs, metrics)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for s in setores:
                cur.execute(
                    sql,
                    (
                        run_id,
                        s.cluster_label,
                        f"CL-{s.cluster_label}",
                        s.centro_lat,
                        s.centro_lon,
                        s.n_pdvs,
                        json.dumps(
                            {
                                "raio_med_km": s.raio_med_km,
                                "raio_p95_km": s.raio_p95_km,
                            }
                        ),
                    ),
                )
                cid = cur.fetchone()[0]
                mapping[s.cluster_label] = cid
            conn.commit()
    return mapping


def salvar_mapeamento_pdvs(
    run_id: int,
    mapping_cluster_id: Dict[int, int],
    labels: List[int],
    pdvs: List[PDV],
):
    """
    Grava o relacionamento PDV → Setor (cluster_setor_pdv)
    """
    sql = """
        INSERT INTO cluster_setor_pdv
            (run_id, cluster_id, pdv_id, lat, lon, cidade, uf)
        VALUES (%s, %s, %s, %s, %s, %s, %s);
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            for pdv, label in zip(pdvs, labels):
                cluster_id = mapping_cluster_id.get(label)
                if cluster_id:
                    cur.execute(
                        sql,
                        (
                            run_id,
                            cluster_id,
                            pdv.id,
                            pdv.lat,
                            pdv.lon,
                            pdv.cidade,
                            pdv.uf,
                        ),
                    )
            conn.commit()

# src/sales_clusterization/infrastructure/persistence/database_writer.py

# ============================================================
# 📦 src/sales_clusterization/infrastructure/persistence/database_writer.py
# ============================================================

import json
import numpy as np
import psycopg2
import os
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Dict
from src.sales_clusterization.domain.entities import Setor, PDV
from src.database.db_connection import get_connection
from loguru import logger


# ============================================================
# 🔧 Adapters para tipos NumPy → psycopg2
# ============================================================
psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.int32, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float64, psycopg2._psycopg.AsIs)
psycopg2.extensions.register_adapter(np.float32, psycopg2._psycopg.AsIs)


# ============================================================
# 🆕 Criação de execução (run)
# ============================================================
def criar_run(
    tenant_id: int,
    uf: str | None,
    cidade: str | None,
    algo: str,
    params: dict,
    descricao: str,
    input_id: str,
    clusterization_id: str,
) -> int:
    """
    Cria um registro de execução (run) na tabela cluster_run vinculado ao tenant.
    Agora inclui:
    - clusterization_id (UUID)
    - descricao (texto descritivo informado pelo usuário)
    - input_id (referência da base de PDVs)
    """

    sql = """
        INSERT INTO cluster_run (
            tenant_id,
            clusterization_id,
            descricao,
            input_id,
            uf,
            cidade,
            algo,
            params,
            status,
            criado_em
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'running', NOW())
        RETURNING id;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                sql,
                (
                    tenant_id,
                    clusterization_id,
                    descricao,
                    input_id,
                    uf,
                    cidade,
                    algo,
                    json.dumps(params, ensure_ascii=False),
                ),
            )
            run_id = cur.fetchone()[0]
            conn.commit()

    logger.info(
        f"🆕 Run criado | tenant={tenant_id} | clusterization_id={clusterization_id} "
        f"| input_id={input_id} | descrição='{descricao}' | UF={uf or 'todas'} | cidade={cidade or 'todas'} | id={run_id}"
    )
    return run_id


# ============================================================
# ✅ Finalização da execução
# ============================================================
def finalizar_run(run_id: int, k_final: int, status: str = "done", error: str | None = None):
    """
    Atualiza o status e o resultado de uma execução (cluster_run).
    """
    sql = """
        UPDATE cluster_run
        SET finished_at = NOW(),
            k_final = %s,
            status = %s,
            error = %s
        WHERE id = %s;
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (int(k_final), status, error, int(run_id)))
            conn.commit()

    logger.info(f"🏁 Run finalizado | id={run_id} | status={status} | k_final={k_final}")


# ============================================================
# 💾 Salvamento de setores (clusters principais)
# ============================================================
def salvar_setores(tenant_id: int, run_id: int, setores: List[Setor]) -> Dict[int, int]:
    """
    Insere setores (macroclusters) e retorna o mapping cluster_label -> cluster_setor.id
    """
    mapping = {}
    sql = """
        INSERT INTO cluster_setor
            (tenant_id, run_id, cluster_label, nome, centro_lat, centro_lon, n_pdvs, metrics)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            for s in setores:
                cluster_label = int(s.cluster_label)
                centro_lat = float(s.centro_lat)
                centro_lon = float(s.centro_lon)
                n_pdvs = int(s.n_pdvs)
                raio_med_km = float(s.raio_med_km)
                raio_p95_km = float(s.raio_p95_km)

                cur.execute(
                    sql,
                    (
                        int(tenant_id),
                        int(run_id),
                        cluster_label,
                        f"CL-{cluster_label}",
                        centro_lat,
                        centro_lon,
                        n_pdvs,
                        json.dumps(
                            {
                                "raio_med_km": raio_med_km,
                                "raio_p95_km": raio_p95_km,
                            },
                            ensure_ascii=False,
                        ),
                    ),
                )
                cid = cur.fetchone()[0]
                mapping[cluster_label] = cid

            conn.commit()

    logger.info(f"💾 {len(mapping)} setores salvos no banco (run_id={run_id})")
    return mapping


# ============================================================
# 🧩 Salvamento do mapeamento PDV → Cluster
# ============================================================
def salvar_mapeamento_pdvs(
    tenant_id: int,
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
            (tenant_id, run_id, cluster_id, pdv_id, lat, lon, cidade, uf)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            count = 0
            for pdv, label in zip(pdvs, labels):
                cluster_id = mapping_cluster_id.get(int(label)) if label is not None else None
                if cluster_id:
                    cur.execute(
                        sql,
                        (
                            int(tenant_id),
                            int(run_id),
                            int(cluster_id),
                            int(pdv.id),
                            float(pdv.lat) if pdv.lat is not None else None,
                            float(pdv.lon) if pdv.lon is not None else None,
                            pdv.cidade,
                            pdv.uf,
                        ),
                    )
                    count += 1
            conn.commit()

    logger.info(f"🧩 {count} PDVs mapeados em clusters (run_id={run_id})")

# ============================================================
# 🧾 Persistência e auditoria de outliers (versão final)
# ============================================================

import math
from src.sales_clusterization.domain.k_estimator import _haversine_km

def salvar_outliers(tenant_id: int, clusterization_id: str, pdv_flags: list):
    """
    Persiste lista de PDVs com flag de outlier (True/False) no banco,
    calcula distância média ao vizinho mais próximo e exporta CSV de auditoria.
    Inclui limpeza automática (DELETE) para reprocessamentos.
    """
    if not pdv_flags:
        logger.warning("⚠️ Nenhum PDV recebido para salvar_outliers().")
        return

    conn = get_connection()
    with conn.cursor() as cur:
        # ============================================================
        # 🔧 Garante estrutura e remove dados antigos (reprocessamento)
        # ============================================================
        cur.execute("""
            CREATE TABLE IF NOT EXISTS sales_clusterization_outliers (
                id SERIAL PRIMARY KEY,
                tenant_id INT NOT NULL,
                clusterization_id UUID NOT NULL,
                pdv_id BIGINT,
                cnpj TEXT,
                cidade TEXT,
                lat DOUBLE PRECISION,
                lon DOUBLE PRECISION,
                distancia_media_km DOUBLE PRECISION,
                is_outlier BOOLEAN DEFAULT FALSE,
                criado_em TIMESTAMP DEFAULT NOW()
            );
        """)

        # Remove registros anteriores do mesmo processamento
        cur.execute(
            "DELETE FROM sales_clusterization_outliers WHERE tenant_id = %s AND clusterization_id = %s;",
            (tenant_id, clusterization_id),
        )

        # ============================================================
        # 📏 Calcula distância média ao vizinho mais próximo (para auditoria)
        # ============================================================
        coords = [(p.lat, p.lon) for p, _ in pdv_flags]
        dist_medias = []
        for i, (lat_a, lon_a) in enumerate(coords):
            vizinhos = [
                _haversine_km((lat_a, lon_a), (lat_b, lon_b))
                for j, (lat_b, lon_b) in enumerate(coords)
                if i != j
            ]
            dist_medias.append(float(np.mean(vizinhos)) if vizinhos else 0.0)

        # ============================================================
        # 💾 Insere registros no banco
        # ============================================================
        rows = [
            (
                tenant_id,
                clusterization_id,
                p.id,
                p.cnpj,
                p.cidade,
                p.lat,
                p.lon,
                dist_medias[i],
                bool(flag),
            )
            for i, (p, flag) in enumerate(pdv_flags)
        ]

        cur.executemany("""
            INSERT INTO sales_clusterization_outliers
            (tenant_id, clusterization_id, pdv_id, cnpj, cidade, lat, lon, distancia_media_km, is_outlier)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, rows)

    conn.commit()
    conn.close()

    total_outliers = sum(1 for _, flag in pdv_flags if flag)
    logger.info(f"🗄️ {len(rows)} registros de outliers gravados no banco para tenant={tenant_id}.")
    logger.success(f"📊 Outliers detectados: {total_outliers} de {len(rows)} PDVs totais ({100*total_outliers/len(rows):.2f}%).")

    # ============================================================
    # 📤 Exporta CSV de auditoria
    # ============================================================
    try:
        base_dir = Path("output/auditoria_outliers") / str(tenant_id)
        base_dir.mkdir(parents=True, exist_ok=True)

        data_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        csv_path = base_dir / f"outliers_{tenant_id}_{clusterization_id}_{data_str}.csv"

        with open(csv_path, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.writer(f, delimiter=";")
            writer.writerow([
                "tenant_id", "clusterization_id", "pdv_id", "cnpj", "cidade",
                "lat", "lon", "distancia_media_km", "is_outlier"
            ])
            for r in rows:
                writer.writerow(r)

        logger.success(f"📁 CSV de auditoria salvo em: {csv_path}")

    except Exception as e:
        logger.warning(f"⚠️ Falha ao exportar CSV de outliers: {e}")

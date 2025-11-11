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
import math
from sklearn.neighbors import NearestNeighbors



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
    Insere setores (macroclusters) e retorna o mapping cluster_label -> cluster_setor.id.

    ✅ Armazena métricas operacionais em colunas dedicadas e também no JSON metrics:
      - raio_med_km / raio_p95_km
      - tempo_medio_min / tempo_max_min
      - distancia_media_km / dist_max_km
    """

    mapping = {}
    sql = """
        INSERT INTO cluster_setor (
            tenant_id,
            run_id,
            cluster_label,
            nome,
            centro_lat,
            centro_lon,
            n_pdvs,
            metrics,
            tempo_medio_min,
            tempo_max_min,
            distancia_media_km,
            dist_max_km,
            subclusters
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING id;
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            for s in setores:
                cluster_label = int(s.cluster_label)
                centro_lat = float(s.centro_lat)
                centro_lon = float(s.centro_lon)
                n_pdvs = int(s.n_pdvs)

                # ============================================================
                # 🧩 Extração de métricas operacionais com fallback seguro
                # ============================================================
                raio_med_km = float(getattr(s, "raio_med_km", 0.0))
                raio_p95_km = float(getattr(s, "raio_p95_km", 0.0))

                # Caso os tempos/distâncias não estejam setados no objeto Setor,
                # calcula dinamicamente com base nos subclusters.
                subclusters = getattr(s, "subclusters", [])
                if subclusters and isinstance(subclusters, list):
                    tempos = [sc.get("tempo_min", 0.0) for sc in subclusters]
                    distancias = [sc.get("dist_km", 0.0) for sc in subclusters]
                    tempo_medio_min = float(np.mean(tempos)) if tempos else 0.0
                    tempo_max_min = float(np.max(tempos)) if tempos else 0.0
                    distancia_media_km = float(np.mean(distancias)) if distancias else 0.0
                    dist_max_km = float(np.max(distancias)) if distancias else 0.0
                else:
                    tempo_medio_min = float(getattr(s, "tempo_medio_min", 0.0))
                    tempo_max_min = float(getattr(s, "tempo_max_min", 0.0))
                    distancia_media_km = float(getattr(s, "distancia_media_km", 0.0))
                    dist_max_km = float(getattr(s, "dist_max_km", 0.0))

                # ============================================================
                # 🧮 JSON consolidado
                # ============================================================
                metrics_json = json.dumps(
                    {
                        "raio_med_km": raio_med_km,
                        "raio_p95_km": raio_p95_km,
                        "tempo_medio_min": tempo_medio_min,
                        "tempo_max_min": tempo_max_min,
                        "distancia_media_km": distancia_media_km,
                        "dist_max_km": dist_max_km,
                        "subclusters": subclusters,
                    },
                    ensure_ascii=False,
                )

                # ============================================================
                # 💾 Inserção no banco
                # ============================================================
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
                        metrics_json,
                        tempo_medio_min,
                        tempo_max_min,
                        distancia_media_km,
                        dist_max_km,
                        json.dumps(subclusters),
                    ),
                )

                cid = cur.fetchone()[0]
                mapping[cluster_label] = cid

            conn.commit()

    logger.info(
        f"💾 {len(mapping)} setores salvos no banco com métricas operacionais "
        f"(run_id={run_id}, tenant={tenant_id})"
    )
    return mapping



# ============================================================
# 🧩 Salvamento do mapeamento PDV → Cluster (ajustada)
# ============================================================
def salvar_mapeamento_pdvs(
    tenant_id: int,
    run_id: int,
    pdvs: List[PDV],
):
    """
    Grava o relacionamento PDV → Setor (cluster_setor_pdv)
    usando o atributo `cluster_id` já atribuído no PDV.
    """
    sql = """
        INSERT INTO cluster_setor_pdv
            (tenant_id, run_id, cluster_id, pdv_id, lat, lon, cidade, uf)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
    """

    with get_connection() as conn:
        with conn.cursor() as cur:
            count = 0
            for p in pdvs:
                if getattr(p, "cluster_id", None):
                    cur.execute(
                        sql,
                        (
                            int(tenant_id),
                            int(run_id),
                            int(p.cluster_id),
                            int(p.id),
                            float(p.lat) if p.lat is not None else None,
                            float(p.lon) if p.lon is not None else None,
                            p.cidade,
                            p.uf,
                        ),
                    )
                    count += 1
            conn.commit()

    logger.info(f"🧩 {count} PDVs mapeados em clusters (run_id={run_id})")


# ============================================================
# 🧾 Persistência e auditoria de outliers (versão compatível)
# ============================================================

def salvar_outliers(tenant_id: int, clusterization_id: str, pdv_flags: list):
    """
    Persiste lista de PDVs com flag de outlier (True/False) no banco.
    🔹 Suporta dois formatos:
       1️⃣ [(PDV, flag)] — modo antigo com objetos PDV
       2️⃣ [{"pdv_id", "lat", "lon", "is_outlier"}] — modo novo normalizado
    🔹 Cálculo de distância média via NearestNeighbors (O(N log N))
    """

    if not pdv_flags:
        logger.warning("⚠️ Nenhum PDV recebido para salvar_outliers().")
        return

    # ============================================================
    # 🧩 Normalização universal do formato de entrada
    # ============================================================
    try:
        if isinstance(pdv_flags[0], tuple):
            # Formato antigo: (PDV, flag)
            rows_dict = [
                {
                    "pdv_id": getattr(p, "id", None),
                    "cnpj": getattr(p, "cnpj", None),
                    "cidade": getattr(p, "cidade", None),
                    "lat": p.lat,
                    "lon": p.lon,
                    "is_outlier": bool(flag),
                }
                for p, flag in pdv_flags
            ]
        else:
            # Formato novo: já é lista de dicionários
            rows_dict = [
                {
                    "pdv_id": r.get("pdv_id"),
                    "cnpj": r.get("cnpj"),
                    "cidade": r.get("cidade"),
                    "lat": r.get("lat"),
                    "lon": r.get("lon"),
                    "is_outlier": bool(r.get("is_outlier", False)),
                }
                for r in pdv_flags
            ]

        logger.info(f"🧾 Outliers normalizados: {len(rows_dict)} registros prontos para gravação.")
    except Exception as e:
        logger.error(f"❌ Erro ao normalizar lista de outliers: {e}")
        return

    # ============================================================
    # 📏 Cálculo eficiente das distâncias médias (em km)
    # ============================================================
    try:
        coords = np.radians(np.array([(r["lat"], r["lon"]) for r in rows_dict if r["lat"] and r["lon"]]))
        n_neighbors = min(6, len(coords))
        nn = NearestNeighbors(n_neighbors=n_neighbors, metric="haversine")
        nn.fit(coords)
        dist, _ = nn.kneighbors(coords)
        dist_medias = dist[:, 1:].mean(axis=1) * 6371.0  # média dos vizinhos (km)
        logger.info(f"📐 Distâncias médias calculadas via NearestNeighbors para {len(coords)} PDVs.")
    except Exception as e:
        logger.error(f"❌ Falha no cálculo de distâncias médias: {e}")
        dist_medias = np.zeros(len(rows_dict))

    # ============================================================
    # 🧩 Inserção no banco
    # ============================================================
    conn = get_connection()
    with conn.cursor() as cur:
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

        cur.execute(
            "DELETE FROM sales_clusterization_outliers WHERE tenant_id = %s AND clusterization_id = %s;",
            (tenant_id, clusterization_id),
        )

        rows = [
            (
                tenant_id,
                clusterization_id,
                r["pdv_id"],
                r.get("cnpj"),
                r.get("cidade"),
                r["lat"],
                r["lon"],
                float(dist_medias[i]) if i < len(dist_medias) else 0.0,
                bool(r["is_outlier"]),
            )
            for i, r in enumerate(rows_dict)
        ]

        cur.executemany("""
            INSERT INTO sales_clusterization_outliers
            (tenant_id, clusterization_id, pdv_id, cnpj, cidade, lat, lon, distancia_media_km, is_outlier)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
        """, rows)

    conn.commit()
    conn.close()

    total_outliers = sum(1 for r in rows_dict if r["is_outlier"])
    logger.info(f"🗄️ {len(rows)} registros de outliers gravados no banco para tenant={tenant_id}.")
    logger.success(
        f"📊 Outliers detectados: {total_outliers} de {len(rows)} PDVs totais "
        f"({100 * total_outliers / len(rows):.2f}%)."
    )

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
            writer.writerows(rows)

        logger.success(f"📁 CSV de auditoria salvo em: {csv_path}")

    except Exception as e:
        logger.warning(f"⚠️ Falha ao exportar CSV de outliers: {e}")




# ============================================================
# 🔄 Classe compatível para uso no ClusterCEPUseCase
# ============================================================

class DatabaseWriter:
    def __init__(self, conn):
        self.conn = conn

    def inserir_mkp_cluster_cep(self, lista_clusters):
        """
        Insere resultado da clusterização de CEPs na tabela mkp_cluster_cep.
        Cada execução tem clusterization_id único (sem sobrescrever execuções anteriores).
        Agora suporta o campo 'modo_clusterizacao' ('ativa' ou 'passiva').
        """
        if not lista_clusters:
            return 0

        from psycopg2.extras import execute_values
        from loguru import logger

        cur = self.conn.cursor()

        # Adiciona campos opcionais no insert
        valores = [
            (
                c["tenant_id"], c["input_id"], c["clusterization_id"], c["uf"], c["cep"],
                c["cluster_id"], c.get("clientes_total", 0), c.get("clientes_target", 0),
                c["lat"], c["lon"], c["cluster_lat"], c["cluster_lon"],
                c["distancia_km"], c["tempo_min"], c["is_outlier"],
                c.get("modo_clusterizacao", "passiva"),
                c.get("centro_nome", ""), c.get("centro_cnpj", ""),
                c.get("cluster_bairro", "") 
            )
            for c in lista_clusters
        ]

        sql = """
            INSERT INTO mkp_cluster_cep (
                tenant_id, input_id, clusterization_id, uf, cep, cluster_id,
                clientes_total, clientes_target,
                lat, lon, cluster_lat, cluster_lon,
                distancia_km, tempo_min, is_outlier,
                modo_clusterizacao, centro_nome, centro_cnpj, cluster_bairro
            )
            VALUES %s
            ON CONFLICT (tenant_id, input_id, clusterization_id, cep) DO UPDATE SET
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                cluster_id = EXCLUDED.cluster_id,
                cluster_lat = EXCLUDED.cluster_lat,
                cluster_lon = EXCLUDED.cluster_lon,
                distancia_km = EXCLUDED.distancia_km,
                tempo_min = EXCLUDED.tempo_min,
                is_outlier = EXCLUDED.is_outlier,
                modo_clusterizacao = EXCLUDED.modo_clusterizacao,
                centro_nome = EXCLUDED.centro_nome,
                centro_cnpj = EXCLUDED.centro_cnpj,
                cluster_bairro = EXCLUDED.cluster_bairro,
                atualizado_em = NOW();
        """



        try:
            logger.info(
                f"💾 Inserindo {len(valores)} linhas em mkp_cluster_cep "
                f"(clusterization_id={lista_clusters[0]['clusterization_id']})"
            )
            execute_values(cur, sql, valores)
            self.conn.commit()

            cur.execute(
                "SELECT COUNT(*) FROM mkp_cluster_cep WHERE clusterization_id = %s;",
                (lista_clusters[0]["clusterization_id"],)
            )
            inseridos = cur.fetchone()[0]

            logger.success(
                f"✅ {inseridos} registros gravados em mkp_cluster_cep "
                f"(clusterization_id={lista_clusters[0]['clusterization_id']})"
            )

            cur.close()
            return inseridos

        except Exception as e:
            self.conn.rollback()
            logger.error(f"❌ Erro ao inserir mkp_cluster_cep: {e}", exc_info=True)
            cur.close()
            return 0

    # ============================================================
    # 💾 Salva endereço no cache de geocodificação
    # ============================================================
    def salvar_cache(self, endereco: str, lat: float, lon: float, tipo: str = "geral"):
        """
        Insere ou atualiza endereço no cache (enderecos_cache).
        Campo 'origem' armazena a fonte dos dados (ex: nominatim, google, etc.).
        """
        if not endereco or lat is None or lon is None:
            return

        try:
            cur = self.conn.cursor()
            cur.execute("""
                INSERT INTO enderecos_cache (endereco, lat, lon, origem, criado_em, atualizado_em)
                VALUES (%s, %s, %s, %s, NOW(), NOW())
                ON CONFLICT (endereco)
                DO UPDATE SET
                    lat = EXCLUDED.lat,
                    lon = EXCLUDED.lon,
                    origem = EXCLUDED.origem,
                    atualizado_em = NOW();
            """, (endereco, lat, lon, tipo))
            self.conn.commit()
            cur.close()
        except Exception as e:
            import logging
            logging.warning(f"⚠️ Erro ao salvar cache de endereço: {e}")

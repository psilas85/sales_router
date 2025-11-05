#sales_router/src/sales_routing/infrastructure/database_writer.py

# ============================================================
# 📦 src/sales_routing/infrastructure/database_writer.py
# ============================================================

import json
import os
import csv
from psycopg2.extras import execute_values
from datetime import datetime
from loguru import logger
from src.database.db_connection import get_connection_context


class SalesRoutingDatabaseWriter:
    """
    Responsável por persistir resultados de subclusterização (rotas diárias)
    e snapshots nomeados ("carteiras") no banco de dados.
    Suporta múltiplas execuções imutáveis identificadas por routing_id (UUID).
    """

    # =========================================================
    # 1️⃣ Salva simulação operacional (modelo VISITAÇÃO)
    # =========================================================
    def salvar_operacional(
        self,
        resultados,
        tenant_id: int,
        run_id: int,
        routing_id: str,
    ):
        """
        Grava nova simulação operacional no banco e gera resumo por cluster.
        Cada routing_id representa uma execução única.
        O tempo e distância mensais são a soma total das rotas (subclusters).
        """
        try:
            logger.info(
                f"💾 Gravando simulação operacional | tenant={tenant_id} | run_id={run_id} | routing_id={routing_id}"
            )

            subcluster_rows, pdv_rows, resumo_rows = [], [], []

            for r in resultados:
                cluster_id = r["cluster_id"]
                k_final = r.get("k_final", 0)
                centro_lat = r.get("centro_lat")
                centro_lon = r.get("centro_lon")

                dist_total_cluster = 0.0
                tempo_total_cluster = 0.0
                total_pdvs_cluster = 0
                qtd_subclusters = len(r["subclusters"])

                for sub in r["subclusters"]:
                    sub_id = sub["subcluster_id"]
                    n_pdvs = sub["n_pdvs"]
                    tempo_total = sub.get("tempo_total_min", 0.0)
                    dist_total = sub.get("dist_total_km", 0.0)
                    rota_coord = sub.get("rota_coord", [])
                    pdvs = sub.get("pdvs", [])

                    # ✅ Corrigido: prioriza o centro do subcluster, fallback no cluster
                    centro_lat = sub.get("centro_lat", r.get("centro_lat"))
                    centro_lon = sub.get("centro_lon", r.get("centro_lon"))

                    dist_total_cluster += dist_total
                    tempo_total_cluster += tempo_total
                    total_pdvs_cluster += n_pdvs

                    rota_coord_json = json.dumps(rota_coord, ensure_ascii=False)

                    subcluster_rows.append(
                        (
                            tenant_id,
                            run_id,
                            routing_id,
                            cluster_id,
                            sub_id,
                            k_final,
                            tempo_total,
                            dist_total,
                            n_pdvs,
                            rota_coord_json,
                            centro_lat,
                            centro_lon,
                            datetime.now(),
                        )
                    )

                    for seq, pdv in enumerate(pdvs, start=1):
                        lat = pdv.get("lat")
                        lon = pdv.get("lon")
                        pdv_id = pdv.get("pdv_id")
                        if lat is None or lon is None:
                            continue

                        pdv_rows.append(
                            (
                                tenant_id,
                                run_id,
                                routing_id,
                                cluster_id,
                                sub_id,
                                pdv_id,
                                seq,
                                lat,
                                lon,
                                datetime.now(),
                            )
                        )

                # =========================================================
                # 📊 Resumo por cluster
                # =========================================================
                if qtd_subclusters > 0:
                    resumo_rows.append(
                        (
                            tenant_id,
                            run_id,
                            routing_id,
                            cluster_id,
                            qtd_subclusters,
                            total_pdvs_cluster,
                            dist_total_cluster,
                            tempo_total_cluster,
                            dist_total_cluster / qtd_subclusters,
                            tempo_total_cluster / qtd_subclusters,
                            dist_total_cluster,   # total mês = soma total
                            tempo_total_cluster,  # total mês = soma total
                            datetime.now(),
                        )
                    )

                    logger.info(
                        f"📦 Cluster {cluster_id}: {qtd_subclusters} rotas | "
                        f"{total_pdvs_cluster} PDVs | {dist_total_cluster:.1f} km | "
                        f"{tempo_total_cluster:.1f} min (total mensal)"
                    )

            # =========================================================
            # 💾 Inserções em batch no banco
            # =========================================================
            with get_connection_context() as conn:
                with conn.cursor() as cur:
                    if subcluster_rows:
                        execute_values(
                            cur,
                            """
                            INSERT INTO sales_subcluster (
                                tenant_id, run_id, routing_id, cluster_id, subcluster_seq,
                                k_final, tempo_total_min, dist_total_km, n_pdvs,
                                rota_coord, centro_lat, centro_lon, criado_em
                            ) VALUES %s
                            """,
                            subcluster_rows,
                        )

                    if pdv_rows:
                        execute_values(
                            cur,
                            """
                            INSERT INTO sales_subcluster_pdv (
                                tenant_id, run_id, routing_id, cluster_id, subcluster_seq,
                                pdv_id, sequencia_ordem, lat, lon, criado_em
                            ) VALUES %s
                            """,
                            pdv_rows,
                        )

                    if resumo_rows:
                        execute_values(
                            cur,
                            """
                            INSERT INTO sales_routing_resumo (
                                tenant_id, run_id, routing_id, cluster_id,
                                qtd_subclusters, qtd_pdvs,
                                dist_total_km, tempo_total_min,
                                dist_media_km, tempo_medio_min,
                                dist_total_mes_km, tempo_total_mes_min,
                                criado_em
                            ) VALUES %s
                            """,
                            resumo_rows,
                        )

                    conn.commit()

            # =========================================================
            # 💾 Exportar resumo CSV (seguro)
            # =========================================================
            try:
                pasta_output = os.path.join("output", "reports", str(tenant_id))
                os.makedirs(pasta_output, exist_ok=True)
                caminho_csv = os.path.join(pasta_output, f"routing_resumo_{routing_id}.csv")

                with open(caminho_csv, "w", newline="", encoding="utf-8") as csvfile:
                    writer = csv.writer(csvfile)
                    writer.writerow([
                        "tenant_id", "run_id", "routing_id", "cluster_id",
                        "qtd_subclusters", "qtd_pdvs",
                        "dist_total_km", "tempo_total_min",
                        "dist_media_km", "tempo_medio_min",
                        "dist_total_mes_km", "tempo_total_mes_min", "criado_em"
                    ])
                    writer.writerows(resumo_rows)

                logger.success(
                    f"✅ Simulação salva | tenant={tenant_id} | routing_id={routing_id} | "
                    f"{len(subcluster_rows)} subclusters | {len(pdv_rows)} PDVs | "
                    f"Resumo exportado → {caminho_csv}"
                )
            except Exception as e:
                logger.warning(f"⚠️ Falha ao exportar CSV de resumo: {e}")

        except Exception as e:
            logger.error(f"❌ Erro ao salvar simulação operacional: {e}")
            raise

    # =========================================================
    # 2️⃣ Cria snapshot (mantido sem alteração)
    # =========================================================
    def salvar_snapshot(self, resultados, tenant_id, nome, descricao, criado_por=None, tags=None):
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                try:
                    logger.info(f"💾 Criando snapshot '{nome}' para tenant {tenant_id}...")

                    tags_json = json.dumps(tags or {}, ensure_ascii=False)
                    uf = tags.get("uf") if tags else None
                    cidade = tags.get("cidade") if tags else None

                    cur.execute(
                        """
                        INSERT INTO sales_routing_snapshot (
                            tenant_id, nome, descricao, criado_por, tags, uf, cidade
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                        RETURNING id;
                        """,
                        (tenant_id, nome, descricao, criado_por, tags_json, uf, cidade),
                    )
                    snapshot_id = cur.fetchone()[0]

                    for r in resultados:
                        for sub in r["subclusters"]:
                            rota = sub.get("rota_coord") or sub.get("rota") or []
                            rota_json = json.dumps(rota, ensure_ascii=False)

                            cur.execute(
                                """
                                INSERT INTO sales_routing_snapshot_subcluster (
                                    snapshot_id, cluster_id, subcluster_seq,
                                    tempo_total_min, dist_total_km, n_pdvs, rota_coord
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s);
                                """,
                                (
                                    snapshot_id,
                                    r["cluster_id"],
                                    sub["subcluster_id"],
                                    sub.get("tempo_total_min", 0.0),
                                    sub.get("dist_total_km", 0.0),
                                    sub.get("n_pdvs", 0),
                                    rota_json,
                                ),
                            )

                            for ordem, p in enumerate(sub.get("pdvs", []), start=1):
                                cur.execute(
                                    """
                                    INSERT INTO sales_routing_snapshot_pdv (
                                        snapshot_id, cluster_id, subcluster_seq,
                                        pdv_id, sequencia_ordem, lat, lon
                                    )
                                    VALUES (%s, %s, %s, %s, %s, %s, %s);
                                    """,
                                    (
                                        snapshot_id,
                                        r["cluster_id"],
                                        sub["subcluster_id"],
                                        p.get("pdv_id"),
                                        ordem,
                                        p.get("lat"),
                                        p.get("lon"),
                                    ),
                                )

                    logger.success(f"✅ Snapshot '{nome}' salvo com sucesso (ID={snapshot_id})")

                except Exception as e:
                    logger.error(f"❌ Erro ao salvar snapshot '{nome}': {e}")
                    raise

    # =========================================================
    # 3️⃣ Atualiza vendedor_id (sem mudanças)
    # =========================================================
    def update_vendedores_operacional(self, tenant_id: int, rotas: list):
        """Atualiza o campo vendedor_id em batch."""
        if not rotas:
            logger.warning("⚠️ Nenhuma rota recebida para atualização de vendedores.")
            return

        rotas_validas = [r for r in rotas if r.get("id") and r.get("vendedor_id") is not None]
        if not rotas_validas:
            logger.warning("⚠️ Nenhuma rota válida para atualização (faltando id ou vendedor_id).")
            return

        from psycopg2.extras import execute_values
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                try:
                    logger.info(
                        f"💾 Atualizando vendedor_id em batch ({len(rotas_validas)} rotas, tenant={tenant_id})..."
                    )

                    cur.execute("CREATE TEMP TABLE tmp_vendedores (id int, vendedor_id int) ON COMMIT DROP;")

                    execute_values(
                        cur,
                        "INSERT INTO tmp_vendedores (id, vendedor_id) VALUES %s",
                        [(r["id"], r["vendedor_id"]) for r in rotas_validas],
                    )

                    cur.execute(
                        """
                        UPDATE sales_subcluster AS s
                        SET vendedor_id = t.vendedor_id
                        FROM tmp_vendedores AS t
                        WHERE s.id = t.id AND s.tenant_id = %s;
                        """,
                        (tenant_id,),
                    )

                    logger.success(
                        f"✅ {len(rotas_validas)} rotas atualizadas com vendedor_id (tenant={tenant_id})."
                    )

                except Exception as e:
                    logger.error(f"❌ Erro ao atualizar vendedor_id: {e}")
                    raise

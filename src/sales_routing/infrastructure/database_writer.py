#sales_routing/infrastructure/database_writer.py

import json
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
from loguru import logger
from src.database.db_connection import get_connection


class SalesRoutingDatabaseWriter:
    """
    Responsável por persistir resultados de subclusterização (rotas diárias)
    e snapshots nomeados ("carteiras") no banco de dados.
    """

    # =========================================================
    # 1. Sobrescreve dados operacionais (sempre a última simulação)
    # =========================================================
    def salvar_operacional(self, resultados, tenant_id: int, run_id: int):
        """
        Substitui a simulação operacional atual do tenant (última execução).
        """
        conn = get_connection()
        cur = conn.cursor()

        try:
            logger.info(f"🧹 Limpando dados anteriores do tenant {tenant_id} em sales_subcluster* ...")
            cur.execute("DELETE FROM sales_subcluster_pdv WHERE tenant_id = %s;", (tenant_id,))
            cur.execute("DELETE FROM sales_subcluster WHERE tenant_id = %s;", (tenant_id,))

            logger.info(f"💾 Gravando nova simulação operacional para tenant {tenant_id}...")

            subcluster_rows = []
            pdv_rows = []

            for r in resultados:
                cluster_id = r["cluster_id"]
                for sub in r["subclusters"]:
                    subcluster_rows.append((
                        tenant_id,
                        run_id,
                        cluster_id,
                        sub["subcluster_id"],
                        r["k_final"],
                        sub["tempo_total_min"],
                        sub["dist_total_km"],
                        sub["n_pdvs"],
                        datetime.now()
                    ))

                    for seq, pdv in enumerate(sub["pdvs"], start=1):
                        pdv_rows.append((
                            tenant_id,
                            run_id,
                            cluster_id,
                            sub["subcluster_id"],
                            pdv["pdv_id"],
                            seq,
                            pdv["lat"],
                            pdv["lon"],
                            datetime.now()
                        ))

            if subcluster_rows:
                execute_values(cur, """
                    INSERT INTO sales_subcluster (
                        tenant_id, run_id, cluster_id, subcluster_seq,
                        k_final, tempo_total_min, dist_total_km, n_pdvs, criado_em
                    ) VALUES %s
                """, subcluster_rows)

            if pdv_rows:
                execute_values(cur, """
                    INSERT INTO sales_subcluster_pdv (
                        tenant_id, run_id, cluster_id, subcluster_seq,
                        pdv_id, sequencia_ordem, lat, lon, criado_em
                    ) VALUES %s
                """, pdv_rows)


            conn.commit()
            logger.success(f"✅ Simulação operacional salva com sucesso para tenant {tenant_id}")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erro ao salvar simulação operacional: {e}")
            raise

        finally:
            cur.close()
            conn.close()

    # =========================================================
    # 2. Cria snapshot / carteira nomeada
    # =========================================================
    def salvar_snapshot(self, resultados, tenant_id, nome, descricao, criado_por=None, tags=None):
        import json
        from loguru import logger
        conn = get_connection()
        cur = conn.cursor()

        try:
            logger.info(f"💾 Criando snapshot '{nome}' para tenant {tenant_id}...")

            tags_json = json.dumps(tags or {}, ensure_ascii=False)
            uf = tags.get("uf") if tags else None
            cidade = tags.get("cidade") if tags else None

            cur.execute("""
                INSERT INTO sales_routing_snapshot (
                    tenant_id, nome, descricao, criado_por, tags, uf, cidade
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (tenant_id, nome, descricao, criado_por, tags_json, uf, cidade))

            snapshot_id = cur.fetchone()[0]

            # Salva subclusters
            for r in resultados:
                for sub in r["subclusters"]:
                    cur.execute("""
                        INSERT INTO sales_routing_snapshot_subcluster (
                            snapshot_id, cluster_id, subcluster_seq,
                            tempo_total_min, dist_total_km, n_pdvs
                        )
                        VALUES (%s, %s, %s, %s, %s, %s);
                    """, (snapshot_id, r["cluster_id"], sub["subcluster_id"],
                        sub["tempo_total_min"], sub["dist_total_km"], sub["n_pdvs"]))

                    # Salva PDVs do subcluster
                    for ordem, p in enumerate(sub["pdvs"], start=1):
                        cur.execute("""
                            INSERT INTO sales_routing_snapshot_pdv (
                                snapshot_id, cluster_id, subcluster_seq,
                                pdv_id, sequencia_ordem, lat, lon
                            )
                            VALUES (%s, %s, %s, %s, %s, %s, %s);
                        """, (
                            snapshot_id,
                            r["cluster_id"],
                            sub["subcluster_id"],
                            p["pdv_id"],
                            ordem,
                            p["lat"],
                            p["lon"]
                        ))


            conn.commit()
            logger.success(f"✅ Snapshot '{nome}' salvo com sucesso (ID={snapshot_id})")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erro ao salvar snapshot '{nome}': {e}")
            raise

        finally:
            cur.close()
            conn.close()

    # =========================================================
    # 3. Excluir snapshot por ID (usado no CLI --excluir)
    # =========================================================
    def delete_snapshot(self, snapshot_id: int):
        """
        Exclui permanentemente um snapshot e seus PDVs/subclusters relacionados.
        """
        conn = get_connection()
        cur = conn.cursor()

        try:
            cur.execute("DELETE FROM sales_routing_snapshot WHERE id = %s;", (snapshot_id,))
            conn.commit()
            logger.success(f"🗑️ Snapshot ID={snapshot_id} excluído com sucesso.")
        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erro ao excluir snapshot ID={snapshot_id}: {e}")
            raise
        finally:
            cur.close()
            conn.close()


    def restore_snapshot_operacional(self, tenant_id, subclusters, pdvs):
        """
        Restaura um snapshot salvo para o modo operacional.
        """
        conn = get_connection()
        cur = conn.cursor()
        try:
            logger.info(f"🧹 Limpando dados anteriores do tenant {tenant_id} em sales_subcluster* ...")
            cur.execute("DELETE FROM sales_subcluster_pdv WHERE tenant_id = %s;", (tenant_id,))
            cur.execute("DELETE FROM sales_subcluster WHERE tenant_id = %s;", (tenant_id,))

            logger.info(f"💾 Restaurando subclusters e PDVs...")
            for s in subclusters:
                cur.execute("""
                    INSERT INTO sales_subcluster (
                        tenant_id, cluster_id, subcluster_seq, k_final,
                        tempo_total_min, dist_total_km, n_pdvs
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (tenant_id, s["cluster_id"], s["subcluster_seq"], s["k_final"],
                    s["tempo_total_min"], s["dist_total_km"], s["n_pdvs"]))

            for p in pdvs:
                cur.execute("""
                    INSERT INTO sales_subcluster_pdv (
                        tenant_id, cluster_id, subcluster_seq, pdv_id, sequencia_ordem
                    ) VALUES (%s, %s, %s, %s, %s);
                """, (tenant_id, p["cluster_id"], p["subcluster_seq"], p["pdv_id"], p["sequencia_ordem"]))

            conn.commit()
            logger.success(f"✅ Snapshot restaurado com sucesso para tenant {tenant_id}")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erro ao restaurar snapshot: {e}")
            raise

        finally:
            cur.close()
            conn.close()

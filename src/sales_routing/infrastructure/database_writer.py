# sales_router/src/sales_routing/infrastructure/database_writer.py

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
    # 1. Sobrescreve dados operacionais (última simulação)
    # =========================================================
    def salvar_operacional(self, resultados, tenant_id: int, run_id: int):
        """
        Substitui a simulação operacional atual do tenant (última execução).
        Inclui gravação do campo rota_coord com rota real via OSRM (multi-stop),
        ajustando o tempo total com tempos de parada e descarregamento.
        """
        from src.sales_routing.application.route_distance_service import RouteDistanceService

        conn = get_connection()
        cur = conn.cursor()

        try:
            logger.info(f"🧹 Limpando dados anteriores do tenant {tenant_id} em sales_subcluster* ...")
            cur.execute("DELETE FROM sales_subcluster_pdv WHERE tenant_id = %s;", (tenant_id,))
            cur.execute("DELETE FROM sales_subcluster WHERE tenant_id = %s;", (tenant_id,))

            logger.info(f"💾 Gravando nova simulação operacional com rotas reais para tenant {tenant_id}...")

            subcluster_rows = []
            pdv_rows = []

            # Instancia o serviço de cálculo de rota (OSRM)
            distance_service = RouteDistanceService()

            for r in resultados:
                cluster_id = r["cluster_id"]

                for sub in r["subclusters"]:
                    # ============================================================
                    # 1️⃣ Monta lista de coordenadas dos PDVs na sequência da rota
                    # ============================================================
                    pdv_coords = [
                        (pdv.get("lat"), pdv.get("lon"))
                        for pdv in sub["pdvs"]
                        if pdv.get("lat") and pdv.get("lon")
                    ]

                    # ============================================================
                    # 2️⃣ Calcula rota completa via OSRM
                    # ============================================================
                    try:
                        rota_final = distance_service.get_full_route(pdv_coords)
                        rota_coord = rota_final["rota_coord"]
                        dist_km = rota_final["distancia_km"]
                        tempo_min = rota_final["tempo_min"]

                        # ============================================================
                        # 3️⃣ Ajusta tempo com paradas e descarregamento
                        # ============================================================
                        n_pdvs = sub["n_pdvs"]
                        peso_total = sum(p.get("cte_peso", 0) or 0 for p in sub["pdvs"])
                        volumes_total = sum(p.get("cte_volumes", 0) or 0 for p in sub["pdvs"])

                        # Tempo de parada por PDV
                        if peso_total > 200:
                            tempo_min += n_pdvs * 20  # minutos
                        else:
                            tempo_min += n_pdvs * 10

                        # Tempo de descarregamento por volume
                        tempo_min += volumes_total * 0.4

                        logger.debug(
                            f"🗺️ Cluster {cluster_id} / Sub {sub['subcluster_id']}: "
                            f"{len(rota_coord)} pts / {dist_km:.2f} km / {tempo_min:.1f} min "
                            f"(ajustado com {n_pdvs} PDVs, {peso_total:.1f} kg, {volumes_total} vol)"
                        )

                    except Exception as e:
                        # fallback mínimo
                        logger.warning(f"⚠️ Falha ao gerar rota OSRM completa: {e}")
                        rota_coord = [{"lat": lat, "lon": lon} for lat, lon in pdv_coords]
                        dist_km = sub.get("dist_total_km", 0)
                        tempo_min = sub.get("tempo_total_min", 0)

                    rota_coord_json = json.dumps(rota_coord, ensure_ascii=False)

                    # ============================================================
                    # 4️⃣ Adiciona registro do subcluster
                    # ============================================================
                    subcluster_rows.append((
                        tenant_id,
                        run_id,
                        cluster_id,
                        sub["subcluster_id"],
                        r["k_final"],
                        tempo_min,
                        dist_km,
                        sub["n_pdvs"],
                        rota_coord_json,
                        datetime.now()
                    ))

                    # ============================================================
                    # 5️⃣ Adiciona PDVs da rota (em ordem)
                    # ============================================================
                    for seq, pdv in enumerate(sub["pdvs"], start=1):
                        pdv_rows.append((
                            tenant_id,
                            run_id,
                            cluster_id,
                            sub["subcluster_id"],
                            pdv["pdv_id"],
                            seq,
                            pdv.get("lat"),
                            pdv.get("lon"),
                            datetime.now()
                        ))

            # ============================================================
            # 6️⃣ Inserções em batch
            # ============================================================
            if subcluster_rows:
                execute_values(cur, """
                    INSERT INTO sales_subcluster (
                        tenant_id, run_id, cluster_id, subcluster_seq,
                        k_final, tempo_total_min, dist_total_km, n_pdvs, rota_coord, criado_em
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
            logger.success(f"✅ Simulação operacional salva com sucesso (tenant {tenant_id}, {len(subcluster_rows)} subclusters)")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erro ao salvar simulação operacional: {e}")
            raise

        finally:
            try:
                distance_service.close()
            except Exception:
                pass
            cur.close()
            conn.close()

    # =========================================================
    # 2️⃣ Cria snapshot / carteira nomeada
    # =========================================================
    def salvar_snapshot(self, resultados, tenant_id, nome, descricao, criado_por=None, tags=None):
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

            # ---------------------------------------------------------
            # Salva subclusters e PDVs
            # ---------------------------------------------------------
            for r in resultados:
                for sub in r["subclusters"]:
                    rota = sub.get("rota_coord") or sub.get("rota") or {}
                    if isinstance(rota, dict) and "coordinates" in rota:
                        rota_coord_json = json.dumps(rota["coordinates"], ensure_ascii=False)
                    elif isinstance(rota, list):
                        rota_coord_json = json.dumps(rota, ensure_ascii=False)
                    else:
                        rota_coord_json = json.dumps([], ensure_ascii=False)

                    cur.execute("""
                        INSERT INTO sales_routing_snapshot_subcluster (
                            snapshot_id, cluster_id, subcluster_seq,
                            tempo_total_min, dist_total_km, n_pdvs, rota_coord
                        )
                        VALUES (%s, %s, %s, %s, %s, %s, %s);
                    """, (
                        snapshot_id, r["cluster_id"], sub["subcluster_id"],
                        sub["tempo_total_min"], sub["dist_total_km"],
                        sub["n_pdvs"], rota_coord_json
                    ))

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
                            p.get("lat"),
                            p.get("lon")
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
    # 3. Excluir snapshot
    # =========================================================
    def delete_snapshot(self, snapshot_id: int):
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

    # =========================================================
    # 4. Restaurar snapshot para modo operacional
    # =========================================================
    def restore_snapshot_operacional(self, tenant_id, subclusters, pdvs, run_id=None):
        """
        Restaura snapshot salvo para o modo operacional.
        Se o snapshot não tiver run_id, será usado 0 (neutro).
        """
        conn = get_connection()
        cur = conn.cursor()
        try:
            logger.info(f"🧹 Limpando dados anteriores do tenant {tenant_id} em sales_subcluster* ...")
            cur.execute("DELETE FROM sales_subcluster_pdv WHERE tenant_id = %s;", (tenant_id,))
            cur.execute("DELETE FROM sales_subcluster WHERE tenant_id = %s;", (tenant_id,))

            logger.info(f"💾 Restaurando subclusters e PDVs...")

            # Usa 0 caso o run_id não seja conhecido
            run_id = run_id or 0

            for s in subclusters:
                cur.execute("""
                    INSERT INTO sales_subcluster (
                        tenant_id, run_id, cluster_id, subcluster_seq, k_final,
                        tempo_total_min, dist_total_km, n_pdvs, rota_coord, criado_em
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                """, (
                    tenant_id, run_id, s["cluster_id"], s["subcluster_seq"], s["k_final"],
                    s["tempo_total_min"], s["dist_total_km"], s["n_pdvs"],
                    json.dumps(s.get("rota_coord", []), ensure_ascii=False),
                    datetime.now()
                ))

            for p in pdvs:
                cur.execute("""
                    INSERT INTO sales_subcluster_pdv (
                        tenant_id, run_id, cluster_id, subcluster_seq,
                        pdv_id, sequencia_ordem, criado_em
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s);
                """, (
                    tenant_id, run_id, p["cluster_id"], p["subcluster_seq"],
                    p["pdv_id"], p["sequencia_ordem"], datetime.now()
                ))

            conn.commit()
            logger.success(f"✅ Snapshot restaurado com sucesso para tenant {tenant_id}")

        except Exception as e:
            conn.rollback()
            logger.error(f"❌ Erro ao restaurar snapshot: {e}")
            raise

        finally:
            cur.close()
            conn.close()

#sales_router/src/sales_routing/infrastructure/database_writer.py

import json
from psycopg2.extras import execute_values
from datetime import datetime
from loguru import logger
import os    
from src.database.db_connection import get_connection_context
from src.sales_routing.application.route_distance_service import RouteDistanceService


class SalesRoutingDatabaseWriter:
    """
    Responsável por persistir resultados de subclusterização (rotas diárias)
    e snapshots nomeados ("carteiras") no banco de dados.
    Agora com fechamento automático de conexão (context manager).
    """

    # =========================================================
    # 1️⃣ Salva simulação operacional (modelo VISITAÇÃO)
    # =========================================================
    def salvar_operacional(self, resultados, tenant_id: int, run_id: int):
        """
        Grava nova simulação operacional no banco.
        Tempo de serviço e velocidade são herdados do CLI (com defaults).
        """
        distance_service = RouteDistanceService()

        try:
            logger.info(f"💾 Gravando nova simulação operacional (VISITAÇÃO) para tenant {tenant_id}...")

            # Recupera parâmetros globais herdados do CLI
            service_min = float(os.getenv("SERVICE_MIN", 0)) or globals().get("SERVICE_MIN_ARG", 20.0)
            velocidade_kmh = float(os.getenv("VEL_KMH", 0)) or globals().get("VEL_KMH_ARG", 30.0)
            logger.info(f"🕒 Parâmetros ativos → Serviço={service_min:.1f} min/PDV | Velocidade={velocidade_kmh:.1f} km/h")

            subcluster_rows, pdv_rows = [], []

            for r in resultados:
                cluster_id = r["cluster_id"]
                for sub in r["subclusters"]:
                    pdvs = sub["pdvs"]
                    centro_lat = r.get("centro_lat")
                    centro_lon = r.get("centro_lon")

                    # Corrige inversões lat/lon
                    for p in pdvs:
                        if abs(p["lat"]) > abs(p["lon"]):
                            p["lat"], p["lon"] = p["lon"], p["lat"]

                    coords = [(float(p["lat"]), float(p["lon"])) for p in pdvs if p.get("lat") and p.get("lon")]
                    if len(coords) == 1 and centro_lat and centro_lon:
                        coords = [(centro_lat, centro_lon)] + coords

                    # Cálculo de rota
                    if len(coords) < 2:
                        rota_coord = [{"lat": coords[0][0], "lon": coords[0][1]}] if coords else []
                        dist_km, tempo_transito, fonte_rota = 0.0, 10.0, "haversine"
                    else:
                        try:
                            rota_final = distance_service.get_full_route(coords)
                            rota_coord = rota_final["rota_coord"]
                            dist_km = rota_final["distancia_km"]
                            tempo_transito = rota_final["tempo_min"]
                            fonte_rota = rota_final.get("fonte", "osrm")
                        except Exception as e:
                            logger.warning(f"⚠️ Sub {sub['subcluster_id']}: falha OSRM ({e}) → fallback haversine.")
                            dist_km = distance_service._haversine_km(coords[0], coords[-1])
                            tempo_transito = (dist_km / velocidade_kmh) * 60
                            rota_coord = [{"lat": c[0], "lon": c[1]} for c in coords]
                            fonte_rota = "haversine"

                    n_pdvs = sub["n_pdvs"]
                    tempo_total = tempo_transito + (n_pdvs * service_min)
                    rota_coord_json = json.dumps(rota_coord, ensure_ascii=False)

                    logger.debug(
                        f"🗺️ Cluster {cluster_id}/Sub {sub['subcluster_id']}: {dist_km:.2f} km | "
                        f"{tempo_total:.1f} min (fonte={fonte_rota})"
                    )

                    subcluster_rows.append((tenant_id, run_id, cluster_id, sub["subcluster_id"], r["k_final"],
                                            tempo_total, dist_km, sub["n_pdvs"], rota_coord_json, datetime.now()))

                    for seq, pdv in enumerate(pdvs, start=1):
                        pdv_rows.append((tenant_id, run_id, cluster_id, sub["subcluster_id"],
                                        pdv["pdv_id"], seq, pdv.get("lat"), pdv.get("lon"), datetime.now()))

            # Inserções em batch
            with get_connection_context() as conn:
                with conn.cursor() as cur:
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

            logger.success(f"✅ Simulação operacional salva ({len(subcluster_rows)} subclusters).")

        except Exception as e:
            logger.error(f"❌ Erro ao salvar simulação operacional: {e}")
            raise
        finally:
            distance_service.close()

    # =========================================================
    # 2️⃣ Cria snapshot
    # =========================================================
    def salvar_snapshot(self, resultados, tenant_id, nome, descricao, criado_por=None, tags=None):
        with get_connection_context() as conn:
            with conn.cursor() as cur:
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

                    for r in resultados:
                        for sub in r["subclusters"]:
                            rota = sub.get("rota_coord") or sub.get("rota") or []
                            rota_json = json.dumps(rota, ensure_ascii=False)

                            cur.execute("""
                                INSERT INTO sales_routing_snapshot_subcluster (
                                    snapshot_id, cluster_id, subcluster_seq,
                                    tempo_total_min, dist_total_km, n_pdvs, rota_coord
                                )
                                VALUES (%s, %s, %s, %s, %s, %s, %s);
                            """, (
                                snapshot_id,
                                r["cluster_id"],
                                sub["subcluster_id"],
                                sub["tempo_total_min"],
                                sub["dist_total_km"],
                                sub["n_pdvs"],
                                rota_json
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

                    logger.success(f"✅ Snapshot '{nome}' salvo com sucesso (ID={snapshot_id})")

                except Exception as e:
                    logger.error(f"❌ Erro ao salvar snapshot '{nome}': {e}")
                    raise

    # =========================================================
    # 3️⃣ Excluir snapshot
    # =========================================================
    def delete_snapshot(self, snapshot_id: int):
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("DELETE FROM sales_routing_snapshot WHERE id = %s;", (snapshot_id,))
                    logger.success(f"🗑️ Snapshot ID={snapshot_id} excluído com sucesso.")
                except Exception as e:
                    logger.error(f"❌ Erro ao excluir snapshot ID={snapshot_id}: {e}")
                    raise

    # =========================================================
    # 4️⃣ Restaurar snapshot
    # =========================================================
    def restore_snapshot_operacional(self, tenant_id, subclusters, pdvs, run_id=None):
        run_id = run_id or 0
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                try:
                    logger.info(f"💾 Restaurando snapshot para tenant {tenant_id}...")

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

                    logger.success(f"✅ Snapshot restaurado com sucesso para tenant {tenant_id}")

                except Exception as e:
                    logger.error(f"❌ Erro ao restaurar snapshot: {e}")
                    raise

    # =========================================================
    # 5️⃣ Atualiza vendedor_id (versão otimizada)
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

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                try:
                    logger.info(f"💾 Atualizando vendedor_id em batch ({len(rotas_validas)} rotas, tenant={tenant_id})...")

                    # 🔹 Cria tabela temporária descartada automaticamente ao final da transação
                    cur.execute("CREATE TEMP TABLE tmp_vendedores (id int, vendedor_id int) ON COMMIT DROP;")

                    # 🔹 Insere os valores em batch
                    execute_values(
                        cur,
                        "INSERT INTO tmp_vendedores (id, vendedor_id) VALUES %s",
                        [(r["id"], r["vendedor_id"]) for r in rotas_validas]
                    )

                    # 🔹 Atualiza as rotas
                    cur.execute("""
                        UPDATE sales_subcluster AS s
                        SET vendedor_id = t.vendedor_id
                        FROM tmp_vendedores AS t
                        WHERE s.id = t.id AND s.tenant_id = %s;
                    """, (tenant_id,))

                    logger.success(f"✅ {len(rotas_validas)} rotas atualizadas com vendedor_id (tenant={tenant_id}).")

                except Exception as e:
                    logger.error(f"❌ Erro ao atualizar vendedor_id: {e}")
                    raise

# src/sales_routing/visualization/route_plotting.py

import folium
import argparse
from pathlib import Path
from loguru import logger
from src.database.db_connection import get_connection


# =========================================================
# 1. BUSCAS DE DADOS
# =========================================================

def buscar_rotas_operacionais(tenant_id: int):
    """
    Busca rotas da simulação operacional (última execução do tenant)
    """
    sql = """
        SELECT s.cluster_id, s.subcluster_seq, p.lat, p.lon, p.sequencia_ordem
        FROM sales_subcluster s
        JOIN sales_subcluster_pdv p
          ON p.cluster_id = s.cluster_id AND p.subcluster_seq = s.subcluster_seq
        WHERE s.tenant_id = %s
        ORDER BY s.cluster_id, s.subcluster_seq, p.sequencia_ordem;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (tenant_id,))
        rows = cur.fetchall()
    conn.close()
    return rows


def buscar_rotas_snapshot(snapshot_id: int):
    """
    Busca rotas de uma carteira salva (snapshot)
    """
    sql = """
        SELECT s.cluster_id, s.subcluster_seq, p.lat, p.lon, p.sequencia_ordem
        FROM sales_routing_snapshot_subcluster s
        JOIN sales_routing_snapshot_pdv p
          ON p.cluster_id = s.cluster_id AND p.subcluster_seq = s.subcluster_seq
        WHERE s.snapshot_id = %s
        ORDER BY s.cluster_id, s.subcluster_seq, p.sequencia_ordem;
    """
    conn = get_connection()
    with conn.cursor() as cur:
        cur.execute(sql, (snapshot_id,))
        rows = cur.fetchall()
    conn.close()
    return rows


# =========================================================
# 2. FUNÇÃO DE PLOTAGEM GENÉRICA
# =========================================================

def gerar_mapa_rotas(dados, output_path: Path, label_prefix: str):
    """
    Gera o mapa HTML interativo com rotas (subclusters)
    """
    if output_path.exists():
        output_path.unlink()  # sempre sobrescreve

    if not dados:
        logger.warning("❌ Nenhum dado de rota encontrado para os parâmetros informados.")
        return

    m = folium.Map(location=[-15.78, -47.93], zoom_start=5)
    rotas = {}

    for cluster_id, sub_seq, lat, lon, ordem in dados:
        rotas.setdefault((cluster_id, sub_seq), []).append((lat, lon))

    for (cluster_id, sub_seq), pontos in rotas.items():
        cor = f"#{hash((cluster_id, sub_seq)) % 0xFFFFFF:06x}"

        folium.PolyLine(
            locations=pontos,
            color=cor,
            weight=3,
            opacity=0.7,
            tooltip=f"{label_prefix} Cluster {cluster_id} - Subcluster {sub_seq}"
        ).add_to(m)

        for lat, lon in pontos:
            folium.CircleMarker(
                location=(lat, lon),
                radius=2,
                color=cor,
                fill=True,
                fill_opacity=0.8
            ).add_to(m)

    m.save(output_path)
    logger.success(f"✅ Mapa salvo em {output_path}")


# =========================================================
# 3. MAIN CLI
# =========================================================

def main():
    parser = argparse.ArgumentParser(description="Gerar mapa de rotas (subclusters ou snapshots)")
    parser.add_argument("--tenant_id", type=int, help="ID do tenant para modo operacional")
    parser.add_argument("--snapshot_id", type=int, help="ID do snapshot salvo (modo carteira)")
    args = parser.parse_args()

    if not args.tenant_id and not args.snapshot_id:
        logger.error("❌ É necessário informar --tenant_id (operacional) ou --snapshot_id (carteira salva).")
        return

    output_dir = Path(f"output/maps/{args.tenant_id or 'snapshot'}")
    output_dir.mkdir(parents=True, exist_ok=True)

    if args.snapshot_id:
        logger.info(f"🗺️ Gerando mapa do snapshot ID={args.snapshot_id} ...")
        dados = buscar_rotas_snapshot(args.snapshot_id)
        output_path = output_dir / f"routing_snapshot_{args.snapshot_id}.html"
        gerar_mapa_rotas(dados, output_path, label_prefix="Snapshot")

    elif args.tenant_id:
        logger.info(f"🗺️ Gerando mapa da simulação operacional (tenant_id={args.tenant_id}) ...")
        dados = buscar_rotas_operacionais(args.tenant_id)
        output_path = output_dir / "routing_operacional.html"
        gerar_mapa_rotas(dados, output_path, label_prefix="Operacional")


if __name__ == "__main__":
    main()

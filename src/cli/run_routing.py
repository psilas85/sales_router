#sales_router/src/cli/run_routing.py

import argparse
from src.sales_routing.infrastructure.database_reader import SalesRoutingDatabaseReader
from src.sales_routing.application.adaptive_subcluster_splitter import gerar_subclusters_adaptativo


def main():
    parser = argparse.ArgumentParser(description="Executa geração de rotas diárias (subclusters) a partir dos clusters de vendas")

    parser.add_argument("--uf", required=True, help="UF dos PDVs (ex: SP, CE)")
    parser.add_argument("--cidade", required=True, help="Cidade dos PDVs (ex: Fortaleza)")
    parser.add_argument("--workday", type=int, default=600, help="Tempo máximo de trabalho diário (minutos)")
    parser.add_argument("--routekm", type=float, default=200.0, help="Distância máxima por rota (km)")
    parser.add_argument("--service", type=int, default=15, help="Tempo médio de visita por PDV (minutos)")
    parser.add_argument("--vel", type=float, default=40.0, help="Velocidade média (km/h)")
    parser.add_argument("--alpha", type=float, default=1.4, help="Fator de correção de caminho (curvas/ruas)")

    args = parser.parse_args()

    print("🚀 Iniciando geração de rotas diárias...")
    print(f"📍 Filtros: {args.cidade}/{args.uf}")

    db = SalesRoutingDatabaseReader()

    # Buscar o último run concluído da cidade/UF informada
    run = db.get_last_run_by_location(args.uf, args.cidade)
    if not run:
        print(f"❌ Nenhum run concluído encontrado para {args.cidade}/{args.uf}.")
        return

    print(f"✅ Run encontrado: ID={run['id']} (K={run['k_final']})")

    clusters = db.get_clusters(run["id"])
    pdvs = db.get_pdvs(run["id"])

    print(f"🔹 Clusters carregados: {len(clusters)}")
    print(f"🔹 PDVs carregados: {len(pdvs)}")

    resultados = gerar_subclusters_adaptativo(
        clusters=clusters,
        pdvs=pdvs,
        workday_min=args.workday,
        route_km_max=args.routekm,
        service_min=args.service,
        v_kmh=args.vel,
        alpha_path=args.alpha,
    )

    print("\n=== RESULTADO FINAL ===")
    for r in resultados:
        print(f"Cluster {r['cluster_id']:>3} → {r['k_final']:>2} rotas | "
              f"PDVs={r['total_pdvs']:<3} | MáxTempo={r['max_tempo']:.1f} min | MáxDist={r['max_dist']:.1f} km")

    db.close()


if __name__ == "__main__":
    main()

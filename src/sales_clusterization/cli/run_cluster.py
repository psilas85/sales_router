#sales_clusterization/cli/run_cluster.py

import argparse
from src.sales_clusterization.application.cluster_use_case import executar_clusterizacao


def main():
    parser = argparse.ArgumentParser(description="Executa clusterização de PDVs")

    parser.add_argument("--uf", default=None, help="UF dos PDVs (ex: SP, CE)")
    parser.add_argument("--cidade", default=None, help="Cidade dos PDVs (ex: Fortaleza)")
    parser.add_argument("--algo", default="kmeans", choices=["kmeans", "dbscan"], help="Algoritmo de clusterização")
    parser.add_argument("--k", type=int, default=None, help="K forçado (opcional)")
    parser.add_argument("--dias_uteis", type=int, default=20, help="Dias úteis no ciclo")
    parser.add_argument("--freq", type=int, default=1, help="Frequência mensal de visitas")
    parser.add_argument("--workday", type=int, default=480, help="Tempo máximo de trabalho diário (minutos)")
    parser.add_argument("--routekm", type=float, default=100, help="Distância máxima por rota (km)")
    parser.add_argument("--service", type=int, default=20, help="Tempo médio de visita por PDV (minutos)")
    parser.add_argument("--vel", type=float, default=30.0, help="Velocidade média (km/h)")
    parser.add_argument("--alpha", type=float, default=1.4, help="Fator de correção de caminho (curvas/ruas)")

    args = parser.parse_args()

    result = executar_clusterizacao(
        uf=args.uf,
        cidade=args.cidade,
        algo=args.algo,
        k_forcado=args.k,
        dias_uteis=args.dias_uteis,
        freq=args.freq,
        workday_min=args.workday,
        route_km_max=args.routekm,
        service_min=args.service,
        v_kmh=args.vel,
        alpha_path=args.alpha,
    )

    print("\n=== RESULTADO FINAL ===")
    print(f"run_id: {result['run_id']}")
    print(f"clusters (K): {result['k_final']}")
    print(f"PDVs: {result['n_pdvs']}")
    print("diagnóstico:", result["diagnostico"])


if __name__ == "__main__":
    main()

# sales_router/src/sales_clusterization/cli/run_cluster.py

# ============================================================
# 📦 src/sales_clusterization/cli/run_cluster.py
# ============================================================

import argparse
import uuid
from loguru import logger
from src.sales_clusterization.application.cluster_use_case import executar_clusterizacao


def main():
    parser = argparse.ArgumentParser(
        description="Executa clusterização de PDVs (SalesRouter / multi-tenant)"
    )

    # =============================
    # Parâmetros obrigatórios
    # =============================
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant (empresa)")
    parser.add_argument("--uf", required=True, help="UF dos PDVs (ex: SP, CE, RJ)")
    parser.add_argument(
        "--descricao",
        required=True,
        help="Descrição da clusterização (ex: 'Clusterização SP Outubro')",
    )
    parser.add_argument(
        "--input_id",
        required=True,
        help="ID da base de PDVs (UUID gerado no preprocessing)",
    )

    # =============================
    # Parâmetros opcionais
    # =============================
    parser.add_argument("--cidade", required=False, help="Cidade opcional dos PDVs (ex: Fortaleza)")
    parser.add_argument(
        "--algo",
        default="kmeans",  # 👈 agora é o padrão
        choices=["kmeans_simples", "kmeans", "dbscan", "hibrido"],
        help="Algoritmo de clusterização: kmeans_simples (padrão), kmeans, dbscan ou hibrido."
    )


    parser.add_argument("--k", type=int, default=None, help="K forçado (apenas para KMeans)")

    parser.add_argument("--dias_uteis", type=int, default=20, help="Dias úteis no ciclo")
    parser.add_argument("--freq", type=int, default=1, help="Frequência mensal de visitas")

    parser.add_argument("--workday", type=int, default=5000, help="Tempo máximo de trabalho diário (minutos)")
    parser.add_argument("--routekm", type=float, default=200.0, help="Distância máxima por rota (km)")
    parser.add_argument("--service", type=int, default=30, help="Tempo médio de visita por PDV (minutos)")
    parser.add_argument("--vel", type=float, default=35.0, help="Velocidade média (km/h)")
    parser.add_argument("--alpha", type=float, default=1.4, help="Fator de correção de caminho (curvas/ruas)")

    parser.add_argument(
        "--max_pdv_cluster",
        type=int,
        default=200,
        help="Máximo de PDVs permitidos por cluster (usado no balanceamento híbrido DBSCAN + KMeans)",
    )

    # 🆕 Novo: número máximo de iterações do refinamento
    parser.add_argument(
        "--max_iter",
        type=int,
        default=10,
        help="Número máximo de iterações para refinamento global (padrão=20)",
    )

    # 🧹 Controle de outliers
    parser.add_argument(
        "--excluir_outliers",
        action="store_true",
        help="Exclui PDVs outliers (pontos isolados geograficamente). Por padrão, outliers são incluídos.",
    )

    # 🆕 clusterization_id vindo do job principal (opcional)
    parser.add_argument("--clusterization_id", type=str, required=False, help="ID da clusterização (externo)")

    parser.add_argument(
        "--z_thresh",
        type=float,
        default=1.5,
        help="Fator z-score para detecção de outliers (padrão=3.0). Valores menores tornam a detecção mais sensível (ex: 2.0 ou 1.5).",
    )



    args = parser.parse_args()

    # ============================================================
    # 🌆 Identificação e log inicial
    # ============================================================
    cidade = args.cidade if args.cidade not in (None, "", "None") else None
    msg_ref = f"{args.uf}-{cidade}" if cidade else f"{args.uf} (todas as cidades)"
    clusterization_id = args.clusterization_id or str(uuid.uuid4())

    logger.info(
        f"🚀 Iniciando clusterização | tenant_id={args.tenant_id} | {msg_ref} | "
        f"input_id={args.input_id} | algoritmo={args.algo}"
    )
    logger.info(f"🆕 clusterization_id={clusterization_id} | descrição='{args.descricao}'")
    logger.info(f"🔧 Excluir outliers: {args.excluir_outliers}")

    # Log dos parâmetros operacionais
    logger.debug(
        f"⚙️ Parâmetros operacionais: "
        f"dias_uteis={args.dias_uteis}, freq={args.freq}, "
        f"workday={args.workday}, routekm={args.routekm}, "
        f"service={args.service}, vel={args.vel}, "
        f"alpha={args.alpha}, max_pdv_cluster={args.max_pdv_cluster}, max_iter={args.max_iter}"
    )

    # ============================================================
    # 🧠 Execução principal
    # ============================================================
    result = executar_clusterizacao(
        tenant_id=args.tenant_id,
        uf=args.uf,
        cidade=cidade,
        algo=args.algo,
        k_forcado=args.k,
        dias_uteis=args.dias_uteis,
        freq=args.freq,
        workday_min=args.workday,
        route_km_max=args.routekm,
        service_min=args.service,
        v_kmh=args.vel,
        alpha_path=args.alpha,
        max_pdv_cluster=args.max_pdv_cluster,
        descricao=args.descricao,
        input_id=args.input_id,
        clusterization_id=clusterization_id,
        excluir_outliers=args.excluir_outliers,
        z_thresh=args.z_thresh,
    )

    # ============================================================
    # 📊 Resultado final
    # ============================================================
    print("\n=== RESULTADO FINAL ===")
    print(f"clusterization_id: {result['clusterization_id']}")
    print(f"run_id: {result['run_id']}")
    print(f"clusters (K): {result['k_final']}")
    print(f"PDVs: {result['n_pdvs']}")
    print("diagnóstico:", result["diagnostico"])


if __name__ == "__main__":
    main()

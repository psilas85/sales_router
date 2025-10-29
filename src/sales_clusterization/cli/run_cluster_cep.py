# ============================================================
# 📦 src/sales_clusterization/cli/run_cluster_cep.py
# ============================================================

import argparse
import logging
import uuid
from database.db_connection import get_connection
from sales_clusterization.application.cluster_cep_use_case import ClusterCEPUseCase
from sales_clusterization.infrastructure.persistence.database_reader import DatabaseReader
from sales_clusterization.infrastructure.persistence.database_writer import DatabaseWriter


def main():
    parser = argparse.ArgumentParser(
        description="Executa clusterização de CEPs de marketplace (MKP) com base em coordenadas geográficas."
    )
    parser.add_argument("--tenant", required=True, type=int, help="Tenant ID (inteiro)")
    parser.add_argument("--uf", required=True, help="UF (estado, ex: SP)")
    parser.add_argument("--input_id", required=True, help="ID do input de marketplace pré-processado")
    parser.add_argument("--descricao", required=True, help="Descrição do processamento")
    parser.add_argument("--velocidade_media", required=True, type=float, help="Velocidade média (km/h)")
    parser.add_argument("--tempo_max_min", required=True, type=float, help="Tempo máximo de viagem em minutos")
    parser.add_argument("--clientes_target", action="store_true", help="Usar clientes_target como peso (padrão: clientes_total)")
    parser.add_argument("--excluir_outliers", action="store_true", help="Excluir outliers (padrão: incluir)")
    parser.add_argument("--cidade", required=False, help="Filtrar por município (opcional)")
    parser.add_argument("--ajustar_coordenadas", action="store_true", default=True,
                        help="Aplica jitter leve em coordenadas duplicadas (±0.002°). Padrão: ativo.")
    parser.add_argument("--ceps_max_cluster", required=False, type=int,
                        help="Número máximo de CEPs permitidos por cluster (padrão: sem limite)")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")

    logging.info("🚀 Iniciando clusterização de CEPs do marketplace...")

    # ============================================================
    # 🔗 Conexão e instâncias
    # ============================================================
    conn = get_connection()
    reader = DatabaseReader(conn)
    writer = DatabaseWriter(conn)

    use_case = ClusterCEPUseCase(
        reader=reader,
        writer=writer,
        tenant_id=args.tenant,
        uf=args.uf,
        input_id=args.input_id,
        descricao=args.descricao,
        velocidade_media=args.velocidade_media,
        tempo_max_min=args.tempo_max_min,
        usar_clientes_target=args.clientes_target,
        excluir_outliers=args.excluir_outliers,
        cidade=args.cidade,
        ajustar_coordenadas=args.ajustar_coordenadas
    )

    # ============================================================
    # ▶️ Execução principal
    # ============================================================
    clusterization_id = use_case.execute(ceps_max_cluster=args.ceps_max_cluster)
    if clusterization_id:
        logging.info(f"🏁 Clusterização finalizada com sucesso | clusterization_id={clusterization_id}")
    else:
        logging.warning("⚠️ Nenhum registro gravado — verifique o input ou filtros aplicados.")


if __name__ == "__main__":
    main()

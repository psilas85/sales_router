# ============================================================
# 📦 src/sales_clusterization/cli/run_cluster_cep_balanceado.py
# ============================================================

import argparse
import sys
from loguru import logger
from database.db_connection import get_connection
from sales_clusterization.infrastructure.persistence.database_reader import DatabaseReader
from sales_clusterization.infrastructure.persistence.database_writer import DatabaseWriter
from sales_clusterization.application.cluster_cep_balanceado_use_case import ClusterCEPBalanceadoUseCase


def main():
    parser = argparse.ArgumentParser(description="Executa clusterização balanceada de CEPs.")
    parser.add_argument("--tenant", type=int, required=True)
    parser.add_argument("--uf", type=str, required=True)
    parser.add_argument("--input_id", type=str, required=True)
    parser.add_argument("--descricao", type=str, required=True)
    parser.add_argument("--centros_csv", type=str, required=True)
    parser.add_argument("--velocidade_media", type=float, default=30.0)
    parser.add_argument("--tempo_max_min", type=float, default=60.0)
    parser.add_argument("--min_clientes", type=int, required=True)
    parser.add_argument("--max_clientes", type=int, required=True)
    parser.add_argument("--cidade", type=str, default=None)
    parser.add_argument("--clientes_total", action="store_true")

    args = parser.parse_args()

    logger.remove()
    logger.add(sys.stdout, colorize=True, format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")
    logger.info("🚀 Iniciando clusterização balanceada de CEPs...")

    conn = get_connection()
    reader = DatabaseReader(conn)
    writer = DatabaseWriter(conn)

    use_case = ClusterCEPBalanceadoUseCase(
        reader=reader,
        writer=writer,
        tenant_id=args.tenant,
        uf=args.uf.upper(),
        input_id=args.input_id,
        descricao=args.descricao,
        caminho_centros=args.centros_csv,
        velocidade_media=args.velocidade_media,
        tempo_max_min=args.tempo_max_min,
        cidade=args.cidade,
        usar_clientes_total=args.clientes_total,
        min_clientes=args.min_clientes,
        max_clientes=args.max_clientes,
    )

    resultado = use_case.execute()

    if resultado:
        logger.success(f"🏁 Clusterização balanceada concluída com sucesso!")
        logger.info(f"📊 Clusterization ID: {resultado['clusterization_id']}")
        logger.info(f"⚙️ Limites: min={resultado['min_clientes']} | max={resultado['max_clientes']}")
    else:
        logger.error("❌ Nenhum resultado retornado.")

    conn.close()


if __name__ == "__main__":
    main()

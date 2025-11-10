# ============================================================
# 📦 src/sales_clusterization/cli/run_cluster_cep_ativa.py
# ============================================================

import argparse
import sys
from loguru import logger
from sales_clusterization.application.cluster_cep_ativa_use_case import ClusterCEPAtivaUseCase
from database.db_connection import get_connection
from sales_clusterization.infrastructure.persistence.database_reader import DatabaseReader
from sales_clusterization.infrastructure.persistence.database_writer import DatabaseWriter


def main():
    parser = argparse.ArgumentParser(
        description="Executa clusterização ativa de CEPs (SalesRouter / multi-tenant)"
    )

    # ======================================================
    # 📥 Parâmetros obrigatórios
    # ======================================================
    parser.add_argument("--tenant", type=int, required=True, help="ID do tenant (empresa)")
    parser.add_argument("--uf", type=str, required=True, help="UF obrigatória (ex: CE, SP, RJ)")
    parser.add_argument("--input_id", type=str, required=True, help="UUID da base marketplace (pré-processamento)")
    parser.add_argument("--descricao", type=str, required=True, help="Descrição da clusterização")
    parser.add_argument("--centros_csv", type=str, required=True, help="Caminho do CSV com endereços dos centros")

    # ======================================================
    # ⚙️ Parâmetros opcionais
    # ======================================================
    parser.add_argument("--velocidade_media", type=float, default=30.0, help="Velocidade média (km/h)")
    parser.add_argument("--tempo_max_min", type=float, default=60.0, help="Tempo máximo de rota (min)")
    parser.add_argument("--cidade", type=str, default=None, help="Filtrar marketplace por cidade específica")
    parser.add_argument(
        "--clientes_total",
        action="store_true",
        help="Usar clientes_total como peso (padrão: clientes_target)",
    )


    args = parser.parse_args()

    # ======================================================
    # 🔧 Configuração de log
    # ======================================================
    logger.remove()
    logger.add(sys.stdout, colorize=True, format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")
    logger.info("🚀 Iniciando clusterização ativa de CEPs...")

    # ======================================================
    # 🧩 Inicializa conexão e dependências
    # ======================================================
    conn = get_connection()
    reader = DatabaseReader(conn)
    writer = DatabaseWriter(conn)

    # ======================================================
    # ▶️ Executa caso de uso
    # ======================================================
    use_case = ClusterCEPAtivaUseCase(
        reader=reader,
        writer=writer,
        tenant_id=args.tenant,
        uf=args.uf.upper(),
        input_id=args.input_id,
        descricao=args.descricao,
        velocidade_media=args.velocidade_media,
        tempo_max_min=args.tempo_max_min,
        caminho_centros=args.centros_csv,
        cidade=args.cidade,
        usar_clientes_total=args.clientes_total,
    )

    resultado = use_case.execute()

    # ======================================================
    # ✅ Resultado final
    # ======================================================
    if resultado:
        logger.success(f"🏁 Clusterização ativa concluída com sucesso!")
        logger.info(f"📊 Clusterization ID: {resultado['clusterization_id']}")
        logger.info(f"🧩 Clusters: {resultado['total_clusters']}")
        logger.info(f"📦 CEPs atribuídos: {resultado['total_ceps']}")
        logger.info(f"⚠️ Outliers: {resultado['total_outliers']}")
        logger.info(f"⏱️ Duração: {resultado['duracao_segundos']}s")
    else:
        logger.error("❌ Nenhum resultado retornado pela clusterização ativa.")

    conn.close()


if __name__ == "__main__":
    main()

# ============================================================
# 📦 src/sales_clusterization/cli/run_cluster_cep_ativa.py
# ============================================================

import argparse
import sys
from loguru import logger
from database.db_connection import get_connection
from sales_clusterization.infrastructure.persistence.database_reader import DatabaseReader
from sales_clusterization.infrastructure.persistence.database_writer import DatabaseWriter
from sales_clusterization.application.cluster_cep_ativa_use_case import ClusterCEPAtivaUseCase
from sales_clusterization.application.cluster_cep_balanceado_use_case import ClusterCEPBalanceadoUseCase


def main():
    parser = argparse.ArgumentParser(
        description="Executa clusterização de CEPs (ativa ou balanceada)"
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
    parser.add_argument("--clientes_total", action="store_true", help="Usar clientes_total como filtro (padrão: clientes_target)")
    parser.add_argument("--modo", type=str, default="ativa", choices=["ativa", "balanceada"], help="Modo de clusterização")

    # 🧩 Parâmetros específicos do modo balanceado
    parser.add_argument("--min_ceps", type=int, required=False, help="Número mínimo de CEPs por cluster")
    parser.add_argument("--max_ceps", type=int, required=False, help="Número máximo de CEPs por cluster")
    parser.add_argument("--max_merge_km", type=float, default=20.0, help="Distância máxima (km) para fusão de clusters vizinhos")

    args = parser.parse_args()

    # ======================================================
    # 🔧 Configuração de log
    # ======================================================
    logger.remove()
    logger.add(sys.stdout, colorize=True, format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")
    logger.info(f"🚀 Iniciando clusterização de CEPs (modo={args.modo})...")

    # ======================================================
    # 🧩 Inicializa conexão e dependências
    # ======================================================
    conn = get_connection()
    reader = DatabaseReader(conn)
    writer = DatabaseWriter(conn)

    # ======================================================
    # ▶️ Escolhe modo de execução
    # ======================================================
    if args.modo == "balanceada":
        if not args.min_ceps or not args.max_ceps:
            logger.error("❌ Para o modo balanceado, use também --min_ceps e --max_ceps.")
            sys.exit(1)

        use_case = ClusterCEPBalanceadoUseCase(
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
            min_ceps=args.min_ceps,
            max_ceps=args.max_ceps,
            max_merge_km=args.max_merge_km,
        )

    else:
        # 🔹 Clusterização ativa tradicional
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

    # ======================================================
    # ▶️ Executa
    # ======================================================
    resultado = use_case.execute()

    # ======================================================
    # ✅ Resultado final
    # ======================================================
    if resultado:
        modo_label = args.modo.capitalize()
        logger.success(f"🏁 Clusterização {modo_label} concluída com sucesso!")
        logger.info(f"📊 Clusterization ID: {resultado['clusterization_id']}")
        if args.modo == "balanceada":
            logger.info(
                f"⚙️ Limites: min={resultado['min_ceps']} | max={resultado['max_ceps']} | "
                f"raio fusão={resultado.get('max_merge_km', 20.0)} km"
            )
    else:
        logger.error(f"❌ Nenhum resultado retornado pela clusterização {args.modo}.")

    conn.close()


if __name__ == "__main__":
    main()

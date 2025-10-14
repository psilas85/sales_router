#sales_router/src/sales_routing/reporting/route_summary_cli.py

import argparse
from loguru import logger
from src.database.db_connection import get_connection
from src.sales_routing.reporting.route_summary_service import RouteSummaryService
from src.sales_routing.reporting.exporters.csv_exporter import CSVExporter
from src.sales_routing.reporting.exporters.json_exporter import JSONExporter


def main():
    parser = argparse.ArgumentParser(description="Gera resumo de rotas (SalesRouter)")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--uf", type=str, help="Filtro por UF")
    parser.add_argument("--cidade", type=str, help="Filtro por cidade")
    parser.add_argument("--snapshot", type=str, help="Nome da carteira/snapshot (opcional)")
    parser.add_argument("--saida", type=str, default="output/reports/rotas_resumo.csv", help="Caminho do CSV de saída")
    parser.add_argument("--json", action="store_true", help="Gera também JSON")
    args = parser.parse_args()

    conn = get_connection()
    service = RouteSummaryService(conn)
    df = service.gerar_resumo_rotas(
        tenant_id=args.tenant,
        uf=args.uf,
        cidade=args.cidade,
        snapshot=args.snapshot
    )

    if df.empty:
        logger.warning("❌ Nenhum dado encontrado. Nada será exportado.")
        return

    CSVExporter.export(df, args.saida)

    if args.json:
        json_path = args.saida.replace(".csv", ".json")
        JSONExporter.export(df.to_dict(orient="records"), json_path)

    logger.success("🏁 Relatório de rotas gerado com sucesso.")


if __name__ == "__main__":
    main()

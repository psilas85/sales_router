#src/sales_routing/cli/run_vendedores_summary.py

import argparse
from loguru import logger
from src.sales_routing.reporting.vendedores_summary_service import VendedoresSummaryService

def main():
    parser = argparse.ArgumentParser(description="Gera resumo consolidado de vendedores (CSV + JSON)")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    args = parser.parse_args()

    logger.info(f"📊 Gerando resumo de vendedores | Tenant={args.tenant}")
    service = VendedoresSummaryService(tenant_id=args.tenant)
    service.gerar_relatorio()

if __name__ == "__main__":
    main()

import argparse
from loguru import logger
from src.sales_routing.reporting.rotas_vendedores_export import RotasVendedoresExport

def main():
    parser = argparse.ArgumentParser(description="Exporta todas as rotas com vendedor, distâncias e tempos")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--uf", type=str, help="Filtro por UF (opcional)")
    parser.add_argument("--cidade", type=str, help="Filtro por cidade (opcional, requer UF)")
    args = parser.parse_args()

    logger.info(f"🧭 Iniciando exportação de rotas | Tenant={args.tenant} | UF={args.uf} | Cidade={args.cidade}")
    RotasVendedoresExport(args.tenant).exportar(uf=args.uf, cidade=args.cidade)

if __name__ == "__main__":
    main()

import argparse
from loguru import logger
from src.sales_routing.application.assign_vendedores_service import AssignVendedoresService


def main():
    parser = argparse.ArgumentParser(description="Atribui vendedores às rotas operacionais existentes")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--freq", type=int, default=1, help="Frequência mensal de visita (x/mês)")
    parser.add_argument("--diasuteis", type=int, default=20, help="Dias úteis no mês")
    parser.add_argument("--workday", type=int, default=500, help="Tempo máximo diário de trabalho em minutos (default=480)")
    parser.add_argument("--uf", type=str, help="Filtro por UF (opcional)")
    parser.add_argument("--cidade", type=str, help="Filtro por cidade (opcional, requer UF)")
    args = parser.parse_args()

    logger.info(
        f"🧭 Iniciando atribuição de vendedores | Tenant={args.tenant}"
        + (f" | UF={args.uf}" if args.uf else "")
        + (f" | Cidade={args.cidade}" if args.cidade else "")
        + f" | Workday={args.workday} min | Dias úteis={args.diasuteis}"
    )

    service = AssignVendedoresService(
        tenant_id=args.tenant,
        freq_mensal=args.freq,
        dias_uteis=args.diasuteis,
        workday_min=args.workday
    )
    service.executar(uf=args.uf, cidade=args.cidade)


if __name__ == "__main__":
    main()

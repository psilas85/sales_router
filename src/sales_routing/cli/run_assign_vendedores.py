#sales_router/src/sales_routing/cli/run_assign_vendedores.py

import argparse
import uuid
from loguru import logger
from src.sales_routing.application.assign_vendedores_service import AssignVendedoresService


def main():
    parser = argparse.ArgumentParser(description="Atribui vendedores às rotas operacionais (imutável por assign_id).")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--routing_id", type=str, required=True, help="Routing ID de referência (UUID)")
    parser.add_argument("--descricao", type=str, required=True, help="Descrição da atribuição (máx. 60 caracteres)")
    parser.add_argument("--usuario", type=str, required=True, help="Usuário responsável pela execução")
    parser.add_argument("--freq", type=int, default=1, help="Frequência mensal de visita (x/mês)")
    parser.add_argument("--diasuteis", type=int, default=20, help="Dias úteis no mês")
    parser.add_argument("--workday", type=int, default=500, help="Tempo máximo diário de trabalho (minutos)")
    parser.add_argument("--uf", type=str, help="Filtro por UF (opcional)")
    parser.add_argument("--cidade", type=str, help="Filtro por cidade (opcional, requer UF)")

    args = parser.parse_args()

    assign_id = str(uuid.uuid4())
    logger.info(
        f"🧭 Iniciando atribuição de vendedores | Tenant={args.tenant} | Routing={args.routing_id} "
        f"| Assign={assign_id} | Usuário={args.usuario} | '{args.descricao}'"
    )

    service = AssignVendedoresService(
        tenant_id=args.tenant,
        routing_id=args.routing_id,
        assign_id=assign_id,
        descricao=args.descricao,
        usuario=args.usuario,
        freq_mensal=args.freq,
        dias_uteis=args.diasuteis,
        workday_min=args.workday,
    )
    service.executar(uf=args.uf, cidade=args.cidade)

    logger.success(f"✅ Atribuição concluída | assign_id={assign_id}")


if __name__ == "__main__":
    main()

# sales_router/src/cli/list_snapshots.py

import argparse
from loguru import logger
from src.sales_routing.infrastructure.database_reader import SalesRoutingDatabaseReader


def main():
    parser = argparse.ArgumentParser(description="Lista snapshots (carteiras) salvos de roteirização de vendas.")
    parser.add_argument("--uf", help="UF para filtrar snapshots (ex: CE, SP)")
    parser.add_argument("--cidade", help="Cidade para filtrar snapshots (ex: Fortaleza)")
    parser.add_argument("--tenant", type=int, default=1, help="Tenant ID (padrão = 1)")

    args = parser.parse_args()

    logger.info(f"📂 Listando snapshots para tenant={args.tenant}, UF={args.uf or '*'}, Cidade={args.cidade or '*'}")

    db = SalesRoutingDatabaseReader()
    snapshots = db.list_snapshots(args.tenant, args.uf, args.cidade)

    if not snapshots:
        print("❌ Nenhum snapshot encontrado com os filtros informados.")
        return

    print(f"\n=== SNAPSHOTS ENCONTRADOS ({len(snapshots)}) ===\n")
    for s in snapshots:
        print(f"📦 {s['nome']} (ID={s['id']})")
        print(f"   🗓️  Criado em: {s['criado_em']:%Y-%m-%d %H:%M}")
        print(f"   🌍 {s['uf']}/{s['cidade']}")
        if s.get("descricao"):
            print(f"   📝 {s['descricao']}")
        print("-" * 60)

    db.close()


if __name__ == "__main__":
    main()

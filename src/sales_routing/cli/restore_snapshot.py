# sales_router/src/cli/restore_snapshot.py

import argparse
from loguru import logger
from src.sales_routing.infrastructure.database_reader import SalesRoutingDatabaseReader
from src.sales_routing.infrastructure.database_writer import SalesRoutingDatabaseWriter


def main():
    parser = argparse.ArgumentParser(description="Restaura um snapshot (carteira) salvo para o modo operacional.")
    parser.add_argument("--nome", required=True, help="Nome da carteira a restaurar (exato)")
    parser.add_argument("--tenant", type=int, default=1, help="Tenant ID (padrão = 1)")

    args = parser.parse_args()

    db_reader = SalesRoutingDatabaseReader()
    db_writer = SalesRoutingDatabaseWriter()

    logger.info(f"🔍 Buscando snapshot '{args.nome}' para tenant {args.tenant}...")
    snapshot = db_reader.get_snapshot_by_name(args.tenant, args.nome)

    if not snapshot:
        print(f"❌ Nenhum snapshot encontrado com nome '{args.nome}'.")
        return

    logger.info(f"📦 Restaurando snapshot '{snapshot['nome']}' (ID={snapshot['id']})...")

    subclusters = db_reader.get_snapshot_subclusters(snapshot["id"])
    pdvs = db_reader.get_snapshot_pdvs(snapshot["id"])

    if not subclusters or not pdvs:
        print(f"⚠️ Snapshot '{args.nome}' está vazio ou corrompido.")
        return

    # Sobrescreve dados operacionais
    db_writer.restore_snapshot_operacional(args.tenant, subclusters, pdvs)

    logger.success(f"✅ Snapshot '{args.nome}' restaurado com sucesso para modo operacional.")
    db_reader.close()


if __name__ == "__main__":
    main()

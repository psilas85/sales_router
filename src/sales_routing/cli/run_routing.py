#sales_router/src/sales_routing/cli/run_routing.py

import argparse
from datetime import datetime
from loguru import logger
from src.sales_routing.infrastructure.database_reader import SalesRoutingDatabaseReader
from src.sales_routing.infrastructure.database_writer import SalesRoutingDatabaseWriter
from src.sales_routing.application.adaptive_subcluster_splitter import gerar_subclusters_adaptativo


def main():
    parser = argparse.ArgumentParser(
        description="Executa geração, listagem, restauração ou exclusão de rotas diárias (subclusters) de vendas"
    )

    # -----------------------
    # Modos de operação
    # -----------------------
    parser.add_argument("--listar", action="store_true", help="Lista snapshots (carteiras) salvos")
    parser.add_argument("--restaurar", type=str, help="Restaura uma carteira salva pelo nome exato")
    parser.add_argument("--excluir", type=str, help="Exclui um snapshot (carteira) pelo nome exato")

    # -----------------------
    # Parâmetros operacionais
    # -----------------------
    parser.add_argument("--uf", type=str, help="UF dos PDVs (ex: SP, CE)")
    parser.add_argument("--cidade", type=str, help="Cidade dos PDVs (ex: Fortaleza)")
    parser.add_argument("--workday", type=int, default=600, help="Tempo máximo de trabalho diário (minutos)")
    parser.add_argument("--routekm", type=float, default=200.0, help="Distância máxima por rota (km)")
    parser.add_argument("--service", type=int, default=15, help="Tempo médio de visita por PDV (minutos)")
    parser.add_argument("--vel", type=float, default=40.0, help="Velocidade média (km/h)")
    parser.add_argument("--alpha", type=float, default=1.4, help="Fator de correção de caminho (curvas/ruas)")
    parser.add_argument("--twoopt", action="store_true", help="Ativa heurística 2-Opt para otimização fina da rota")

    # -----------------------
    # Snapshot (carteira)
    # -----------------------
    parser.add_argument("--salvar", type=str, help="Nome da carteira/snapshot (opcional)")
    parser.add_argument("--descricao", type=str, help="Descrição da carteira (opcional)")
    parser.add_argument("--usuario", type=str, default="cli", help="Usuário responsável pela execução")
    parser.add_argument("--tenant", type=int, default=1, help="Tenant ID (padrão = 1)")

    args = parser.parse_args()

    db_reader = SalesRoutingDatabaseReader()
    db_writer = SalesRoutingDatabaseWriter()

    # ======================================================
    # 1️⃣ LISTAR SNAPSHOTS
    # ======================================================
    if args.listar:
        logger.info(f"📂 Listando snapshots para tenant={args.tenant}...")
        snapshots = db_reader.list_snapshots(args.tenant, args.uf, args.cidade)
        if not snapshots:
            print("❌ Nenhum snapshot encontrado.")
        else:
            print(f"\n=== SNAPSHOTS ENCONTRADOS ({len(snapshots)}) ===\n")
            for s in snapshots:
                print(f"📦 {s['nome']} (ID={s['id']})")
                print(f"   🗓️  Criado em: {s['criado_em']:%Y-%m-%d %H:%M}")
                print(f"   🌍 {s.get('uf','-')}/{s.get('cidade','-')}")
                if s.get('descricao'):
                    print(f"   📝 {s['descricao']}")
                print("-" * 60)
        db_reader.close()
        return

    # ======================================================
    # 2️⃣ RESTAURAR SNAPSHOT
    # ======================================================
    if args.restaurar:
        nome = args.restaurar.strip()
        logger.info(f"🔍 Buscando snapshot '{nome}' para tenant {args.tenant}...")
        snapshot = db_reader.get_snapshot_by_name(args.tenant, nome)

        if not snapshot:
            print(f"❌ Nenhum snapshot encontrado com nome '{nome}'.")
            db_reader.close()
            return

        subclusters = db_reader.get_snapshot_subclusters(snapshot["id"])
        pdvs = db_reader.get_snapshot_pdvs(snapshot["id"])

        if not subclusters or not pdvs:
            print(f"⚠️ Snapshot '{nome}' está vazio ou corrompido.")
            db_reader.close()
            return

        db_writer.restore_snapshot_operacional(args.tenant, subclusters, pdvs)
        logger.success(f"✅ Snapshot '{nome}' restaurado com sucesso para tenant {args.tenant}")
        db_reader.close()
        return

    # ======================================================
    # 3️⃣ EXCLUIR SNAPSHOT
    # ======================================================
    if args.excluir:
        nome = args.excluir.strip()
        logger.info(f"🗑️ Solicitada exclusão do snapshot '{nome}' (tenant {args.tenant})...")
        snapshot = db_reader.get_snapshot_by_name(args.tenant, nome)

        if not snapshot:
            print(f"❌ Nenhum snapshot encontrado com nome '{nome}'.")
            db_reader.close()
            return

        confirm = input(f"⚠️ Confirmar exclusão permanente de '{nome}'? (s/N): ").strip().lower()
        if confirm != "s":
            print("❎ Exclusão cancelada pelo usuário.")
            db_reader.close()
            return

        db_writer.delete_snapshot(snapshot["id"])
        logger.success(f"✅ Snapshot '{nome}' excluído com sucesso.")
        db_reader.close()
        return

    # ======================================================
    # 4️⃣ EXECUTAR NOVA SIMULAÇÃO
    # ======================================================
    if not args.uf or not args.cidade:
        print("❌ É necessário informar --uf e --cidade para executar uma simulação.")
        return

    print("\n🚀 Iniciando geração de rotas diárias...")
    print(f"📍 Filtros aplicados: {args.cidade}/{args.uf}")
    print("------------------------------------------------------")

    run = db_reader.get_last_run_by_location(args.uf, args.cidade)
    if not run:
        print(f"❌ Nenhum run concluído encontrado para {args.cidade}/{args.uf}.")
        return

    tenant_id = args.tenant
    run_id = run["id"]

    print(f"✅ Run encontrado: ID={run_id} (K={run['k_final']})")
    clusters = db_reader.get_clusters(run_id)
    pdvs = db_reader.get_pdvs(run_id)
    print(f"🔹 Clusters carregados: {len(clusters)}")
    print(f"🔹 PDVs carregados: {len(pdvs)}")

    resultados = gerar_subclusters_adaptativo(
        clusters=clusters,
        pdvs=pdvs,
        workday_min=args.workday,
        route_km_max=args.routekm,
        service_min=args.service,
        v_kmh=args.vel,
        alpha_path=args.alpha,
        aplicar_two_opt=args.twoopt,
    )

    print("\n💾 Salvando resultados no banco de dados...")
    db_writer.salvar_operacional(resultados, tenant_id, run_id)

    if args.salvar:
        nome = args.salvar.strip()
        descricao = args.descricao or f"Snapshot criado em {datetime.now():%d/%m/%Y %H:%M}"
        db_writer.salvar_snapshot(
            resultados=resultados,
            tenant_id=tenant_id,
            nome=nome,
            descricao=descricao,
            criado_por=args.usuario,
            tags={"uf": args.uf, "cidade": args.cidade}
        )
        print(f"📦 Snapshot '{nome}' salvo com sucesso!\n")

    print("\n🏁 Execução concluída com sucesso!\n")
    db_reader.close()


if __name__ == "__main__":
    main()

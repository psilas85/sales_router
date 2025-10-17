import argparse
from datetime import datetime
from loguru import logger
from src.database.cleanup_service import limpar_dados_operacionais
from src.database.db_connection import get_connection_context
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
    parser.add_argument("--uf", type=str, help="UF dos PDVs (ex: SP, CE, RJ)")
    parser.add_argument("--cidade", type=str, help="Cidade dos PDVs (ex: Fortaleza)")
    parser.add_argument("--workday", type=int, default=600, help="Tempo máximo de trabalho diário (minutos)")
    parser.add_argument("--routekm", type=float, default=100.0, help="Distância máxima por rota (km)")
    parser.add_argument("--service", type=float, default=20.0, help="Tempo médio de visita por PDV (minutos)")
    parser.add_argument("--vel", type=float, default=30.0, help="Velocidade média (km/h)")
    parser.add_argument("--alpha", type=float, default=1.4, help="Fator de correção de caminho (curvas/ruas)")
    parser.add_argument("--twoopt", action="store_true", help="Ativa heurística 2-Opt para otimização fina da rota")

    # -----------------------
    # Snapshot (carteira)
    # -----------------------
    parser.add_argument("--salvar", type=str, help="Nome da carteira/snapshot (opcional)")
    parser.add_argument("--descricao", type=str, help="Descrição da carteira (opcional)")
    parser.add_argument("--usuario", type=str, default="cli", help="Usuário responsável pela execução")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID (obrigatório)")

    args = parser.parse_args()
    tenant_id = args.tenant

    # ======================================================
    # 🔄 Exporta parâmetros para acesso global (usados por serviços internos)
    # ======================================================
    globals()["SERVICE_MIN_ARG"] = args.service
    globals()["VEL_KMH_ARG"] = args.vel
    globals()["ALPHA_PATH_ARG"] = args.alpha

    # ======================================================
    # 🧹 LIMPEZA AUTOMÁTICA DE SIMULAÇÕES OPERACIONAIS
    # ======================================================
    logger.info(f"🧹 Limpando simulações operacionais do tenant_id={tenant_id} antes da nova roteirização...")
    try:
        limpar_dados_operacionais("routing", tenant_id=tenant_id)
    except Exception as e:
        logger.error(f"❌ Falha na limpeza automática: {e}")
        return

    # ======================================================
    # 🔧 Inicialização dos serviços de banco de dados
    # ======================================================
    db_reader = SalesRoutingDatabaseReader()
    db_writer = SalesRoutingDatabaseWriter()

    # ======================================================
    # 1️⃣ LISTAR SNAPSHOTS
    # ======================================================
    if args.listar:
        logger.info(f"📂 Listando snapshots para tenant={tenant_id}...")
        snapshots = db_reader.list_snapshots(tenant_id, args.uf, args.cidade)
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
        return

    # ======================================================
    # 2️⃣ RESTAURAR SNAPSHOT
    # ======================================================
    if args.restaurar:
        nome = args.restaurar.strip()
        logger.info(f"🔍 Buscando snapshot '{nome}' para tenant {tenant_id}...")
        snapshot = db_reader.get_snapshot_by_name(tenant_id, nome)
        if not snapshot:
            print(f"❌ Nenhum snapshot encontrado com nome '{nome}'.")
            return
        subclusters = db_reader.get_snapshot_subclusters(snapshot["id"])
        pdvs = db_reader.get_snapshot_pdvs(snapshot["id"])
        if not subclusters or not pdvs:
            print(f"⚠️ Snapshot '{nome}' está vazio ou corrompido.")
            return
        db_writer.restore_snapshot_operacional(tenant_id, subclusters, pdvs)
        logger.success(f"✅ Snapshot '{nome}' restaurado com sucesso para tenant {tenant_id}")
        return

    # ======================================================
    # 3️⃣ EXCLUIR SNAPSHOT
    # ======================================================
    if args.excluir:
        nome = args.excluir.strip()
        logger.info(f"🗑️ Solicitada exclusão do snapshot '{nome}' (tenant {tenant_id})...")
        snapshot = db_reader.get_snapshot_by_name(tenant_id, nome)
        if not snapshot:
            print(f"❌ Nenhum snapshot encontrado com nome '{nome}'.")
            return
        confirm = input(f"⚠️ Confirmar exclusão permanente de '{nome}'? (s/N): ").strip().lower()
        if confirm != "s":
            print("❎ Exclusão cancelada pelo usuário.")
            return
        db_writer.delete_snapshot(snapshot["id"])
        logger.success(f"✅ Snapshot '{nome}' excluído com sucesso.")
        return

    # ======================================================
    # 4️⃣ EXECUTAR NOVA SIMULAÇÃO DE ROTAS
    # ======================================================
    if not args.uf:
        print("❌ É necessário informar a UF (--uf).")
        return

    # ✅ Se cidade não informada, busca o último run da UF inteira
    if not args.cidade:
        logger.info(f"🌎 Nenhuma cidade especificada — buscando último run concluído da UF={args.uf}")
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT id, uf, cidade, algo, k_final, params
                    FROM cluster_run
                    WHERE status = 'done' AND UPPER(uf) = UPPER(%s)
                    ORDER BY id DESC
                    LIMIT 1;
                """, (args.uf,))
                row = cur.fetchone()
                if not row:
                    print(f"❌ Nenhum run concluído encontrado para UF={args.uf}.")
                    return
                colnames = [desc[0] for desc in cur.description]
                run = dict(zip(colnames, row))
                args.cidade = run.get("cidade")
    else:
        run = db_reader.get_last_run_by_location(args.uf, args.cidade)
        if not run:
            print(f"❌ Nenhum run concluído encontrado para {args.cidade}/{args.uf}.")
            return

    run_id = run["id"]
    cidade_ref = args.cidade or "todas as cidades"
    print(f"\n🚀 Iniciando geração de rotas diárias para {args.uf} ({cidade_ref})...")
    print(f"✅ Run encontrado: ID={run_id} (K={run['k_final']})")
    print("------------------------------------------------------")

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
            tags={"uf": args.uf, "cidade": args.cidade},
        )
        print(f"📦 Snapshot '{nome}' salvo com sucesso!\n")

    print("\n🏁 Execução concluída com sucesso!\n")


if __name__ == "__main__":
    main()

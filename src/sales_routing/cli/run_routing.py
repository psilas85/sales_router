#sales_router/src/sales_routing/cli/run_routing.py

import argparse
import uuid
from datetime import datetime
from loguru import logger
from src.database.db_connection import get_connection_context
from src.sales_routing.infrastructure.database_reader import SalesRoutingDatabaseReader
from src.sales_routing.infrastructure.database_writer import SalesRoutingDatabaseWriter
from src.sales_routing.application.adaptive_subcluster_splitter import gerar_subclusters_adaptativo


def main():
    parser = argparse.ArgumentParser(
        description="Executa geração de rotas diárias (subclusters) sem sobrescrever processamentos anteriores."
    )

    # ======================================================
    # 🔧 PARÂMETROS OBRIGATÓRIOS
    # ======================================================
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID (obrigatório)")
    parser.add_argument("--clusterization_id", type=str, required=True, help="ID da clusterização associada (UUID)")
    parser.add_argument("--descricao", type=str, required=True, help="Descrição da execução (máx. 60 caracteres)")

    # ======================================================
    # ⚙️ PARÂMETROS OPERACIONAIS
    # ======================================================
    parser.add_argument("--uf", type=str, required=True, help="UF dos PDVs (ex: SP, CE, RJ)")
    parser.add_argument("--cidade", type=str, help="Cidade dos PDVs (ex: Fortaleza)")
    parser.add_argument("--workday", type=int, default=600, help="Tempo máximo de trabalho diário (minutos)")
    parser.add_argument("--routekm", type=float, default=100.0, help="Distância máxima por rota (km)")
    parser.add_argument("--service", type=float, default=20.0, help="Tempo médio de visita por PDV (minutos)")
    parser.add_argument("--vel", type=float, default=30.0, help="Velocidade média (km/h)")
    parser.add_argument("--alpha", type=float, default=1.4, help="Fator de correção de caminho (curvas/ruas)")
    parser.add_argument("--twoopt", action="store_true", help="Ativa heurística 2-Opt para otimização fina da rota")
    parser.add_argument("--usuario", type=str, default="cli", help="Usuário responsável pela execução")

    args = parser.parse_args()
    tenant_id = args.tenant

    # ======================================================
    # ✅ VALIDAÇÕES
    # ======================================================
    descricao = args.descricao.strip()
    if len(descricao) == 0 or len(descricao) > 60:
        print("❌ A descrição deve ter entre 1 e 60 caracteres.")
        return

    try:
        uuid.UUID(args.clusterization_id)
    except ValueError:
        print("❌ clusterization_id inválido (deve ser um UUID válido).")
        return

    # ======================================================
    # 🆔 GERAÇÃO DO ROUTING_ID
    # ======================================================
    routing_id = str(uuid.uuid4())
    clusterization_id = args.clusterization_id.strip()
    logger.info(f"🆕 Criando nova execução de roteirização:")
    logger.info(f"   routing_id={routing_id}")
    logger.info(f"   clusterization_id={clusterization_id}")
    logger.info(f"   tenant_id={tenant_id}")
    logger.info(f"   descricao={descricao}")

    # ======================================================
    # 🔧 Inicialização dos serviços de banco de dados
    # ======================================================
    db_reader = SalesRoutingDatabaseReader()
    db_writer = SalesRoutingDatabaseWriter()

    # ======================================================
    # 🧾 REGISTRO DO HISTÓRICO
    # ======================================================
    try:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO historico_subcluster_jobs (
                        tenant_id, routing_id, clusterization_id, descricao, criado_por, criado_em
                    ) VALUES (%s, %s, %s, %s, %s, NOW());
                """, (tenant_id, routing_id, clusterization_id, descricao, args.usuario))
                conn.commit()
        logger.success(f"✅ Registro criado no histórico (routing_id={routing_id})")
    except Exception as e:
        logger.error(f"❌ Falha ao registrar histórico: {e}")
        return

    # ======================================================
    # 🔍 BUSCAR CLUSTERS E PDVs DA CLUSTERIZAÇÃO
    # ======================================================
    run = db_reader.get_last_run_by_location(args.uf, args.cidade)
    if not run:
        print(f"❌ Nenhum run concluído encontrado para {args.cidade or 'UF inteira'} / {args.uf}.")
        return

    run_id = run["id"]
    cidade_ref = args.cidade or "todas as cidades"
    print(f"\n🚀 Iniciando roteirização para {args.uf} ({cidade_ref})...")
    print(f"✅ Clusterização encontrada: ID={run_id} (K={run['k_final']})")
    print(f"🆔 routing_id={routing_id}")
    print("------------------------------------------------------")

    clusters = db_reader.get_clusters(run_id)
    pdvs = db_reader.get_pdvs(run_id)
    print(f"🔹 Clusters carregados: {len(clusters)}")
    print(f"🔹 PDVs carregados: {len(pdvs)}")

    # ======================================================
    # 🧠 GERAÇÃO DOS SUBCLUSTERS E ROTAS
    # ======================================================
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

    # ======================================================
    # 💾 SALVANDO RESULTADOS NO BANCO
    # ======================================================
    print("\n💾 Salvando resultados no banco de dados...")
    try:
        db_writer.salvar_operacional(
            resultados=resultados,
            tenant_id=tenant_id,
            run_id=run_id,
            routing_id=routing_id,
        )
        print(f"✅ Resultados salvos com sucesso (routing_id={routing_id})")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar resultados: {e}")
        return

    # ======================================================
    # ✅ FINALIZAÇÃO
    # ======================================================
    print("\n🏁 Execução concluída com sucesso!")
    print(f"📦 routing_id registrado: {routing_id}\n")


if __name__ == "__main__":
    main()

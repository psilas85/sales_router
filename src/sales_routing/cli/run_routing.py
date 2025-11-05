# ============================================================
# 📦 src/sales_routing/cli/run_routing.py
# ============================================================

import argparse
import uuid
from datetime import datetime
from loguru import logger
from src.database.db_connection import get_connection_context
from src.sales_routing.infrastructure.database_reader import SalesRoutingDatabaseReader
from src.sales_routing.infrastructure.database_writer import SalesRoutingDatabaseWriter
from src.sales_routing.application.adaptive_subcluster_splitter import gerar_subclusters_adaptativo
from src.sales_routing.application.fixed_subcluster_splitter import gerar_subclusters_fixos


def main():
    parser = argparse.ArgumentParser(
        description="🚚 Executa geração de rotas diárias (subclusters) com base em capacidade mensal (dias úteis × frequência)."
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
    parser.add_argument("--service_min", type=float, default=20.0, help="⏱️ Tempo médio de visita por PDV (minutos)")
    parser.add_argument("--v_kmh", type=float, default=30.0, help="🚚 Velocidade média operacional (km/h)")
    parser.add_argument("--alpha_path", type=float, default=1.3, help="📐 Fator de alongamento de rota (α)")
    parser.add_argument("--twoopt", action="store_true", help="Ativa heurística 2-Opt para otimização fina da rota")
    parser.add_argument("--usuario", type=str, default="cli", help="Usuário responsável pela execução")

    # ======================================================
    # 🧮 PARÂMETROS DE CAPACIDADE
    # ======================================================
    parser.add_argument("--dias_uteis", type=int, default=21, help="Dias úteis no mês (padrão=21)")
    parser.add_argument("--frequencia_visita", type=int, default=1, help="Frequência de visita mensal (1=mensal, 2=quinzenal, 4=semanal)")

    # ======================================================
    # 🧠 MODO DE SUBCLUSTERIZAÇÃO
    # ======================================================
    parser.add_argument(
        "--modo",
        choices=["adaptativo", "fixo"],
        default="fixo",
        help="Define o modo de subclusterização: 'adaptativo' (avalia tempo/distância) ou 'fixo' (KMeans direto por dias úteis/frequência)."
    )

    # ======================================================
    # 🔢 MODO DE CÁLCULO DO NÚMERO DE ROTAS
    # ======================================================
    parser.add_argument(
        "--modo_calculo",
        type=str,
        choices=["proporcional", "fixo"],
        default="proporcional",
        help="Modo de cálculo do nº de rotas por cluster: proporcional (padrão) ou fixo (dias_uteis)."
    )

    # ✅ PARSE FINAL
    args = parser.parse_args()
    tenant_id = args.tenant

    # ======================================================
    # ✅ VALIDAÇÕES BÁSICAS
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
    logger.info(f"🆕 Criando execução de roteirização (modo={args.modo})")
    logger.info(f"   routing_id={routing_id}")
    logger.info(f"   clusterization_id={clusterization_id}")
    logger.info(f"   tenant_id={tenant_id}")
    logger.info(f"   descricao={descricao}")
    logger.info(f"   parâmetros: vel={args.v_kmh} km/h | α={args.alpha_path} | service={args.service_min} min")

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
    print(f"\n🧮 Modo selecionado: {args.modo.upper()}")

    if args.modo == "fixo":
        resultados = gerar_subclusters_fixos(
            clusters=clusters,
            pdvs=pdvs,
            dias_uteis=args.dias_uteis,
            freq_padrao=args.frequencia_visita,
            v_kmh=args.v_kmh,
            service_min=args.service_min,
            alpha_path=args.alpha_path,
            aplicar_two_opt=args.twoopt,
            modo_calculo=args.modo_calculo,
        )
    else:
        resultados = gerar_subclusters_adaptativo(
            clusters=clusters,
            pdvs=pdvs,
            dias_uteis=args.dias_uteis,
            freq_padrao=args.frequencia_visita,
            v_kmh=args.v_kmh,
            service_min=args.service_min,
            alpha_path=args.alpha_path,
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
    print(f"📅 Configuração usada: {args.dias_uteis} dias úteis / {args.frequencia_visita}x por mês\n")
    print(f"⚙️ Parâmetros operacionais: {args.v_kmh} km/h | {args.service_min} min/PDV | α={args.alpha_path}\n")
    print(f"🧭 Modo de subclusterização: {args.modo.upper()} | cálculo: {args.modo_calculo.upper()}\n")


if __name__ == "__main__":
    main()

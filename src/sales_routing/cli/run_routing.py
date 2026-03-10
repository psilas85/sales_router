#sales_router/src/sales_routing/cli/run_routing.py

# ============================================================
# 📦 src/sales_routing/cli/run_routing.py — CORRIGIDO (defaults condicionais)
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
from src.sales_routing.application.balanced_subcluster_splitter import gerar_subclusters_balanceados


def aplicar_default(valor, default):
    """Aplica default apenas se o valor não tiver sido passado."""
    return default if valor is None else valor


def main():
    parser = argparse.ArgumentParser(
        description="Executa geração de rotas (subclusters) com base em capacidade mensal."
    )

    # ======================================================
    # PARÂMETROS OBRIGATÓRIOS
    # ======================================================
    parser.add_argument("--tenant", type=int, required=True)
    parser.add_argument("--clusterization_id", type=str, required=True)
    parser.add_argument("--descricao", type=str, required=True)

    # ======================================================
    # PARÂMETROS OPERACIONAIS
    # ======================================================
    parser.add_argument("--uf", type=str, required=True)
    parser.add_argument("--cidade", type=str)

    parser.add_argument("--service_min", type=float, default=None)
    parser.add_argument("--v_kmh", type=float, default=None)
    parser.add_argument("--alpha_path", type=float, default=None)
    parser.add_argument("--twoopt", action="store_true")
    parser.add_argument("--usuario", type=str, default="cli")

    # ======================================================
    # PARÂMETROS DE CAPACIDADE
    # ======================================================
    parser.add_argument("--dias_uteis", type=int, default=None)
    parser.add_argument("--frequencia_visita", type=int, default=None)
    parser.add_argument("--min_pdvs_rota", type=int, default=None)
    parser.add_argument("--max_pdvs_rota", type=int, default=None)

    # ======================================================
    # MODO DE SUBCLUSTERIZAÇÃO
    # ======================================================
    parser.add_argument("--modo", choices=["adaptativo", "fixo", "balanceado"], default=None)

    # ======================================================
    # MODO DE CÁLCULO DE ROTAS
    # ======================================================
    parser.add_argument(
        "--modo_calculo",
        choices=["frequencia", "proporcional", "capacidade"],
        default=None
    )

    args = parser.parse_args()
    tenant_id = args.tenant

    # ======================================================
    # VALIDAÇÕES
    # ======================================================
    descricao = args.descricao.strip()
    if len(descricao) == 0 or len(descricao) > 60:
        print("❌ A descrição deve ter entre 1 e 60 caracteres.")
        return

    try:
        uuid.UUID(args.clusterization_id)
    except ValueError:
        print("❌ clusterization_id inválido.")
        return

    # ======================================================
    # APLICAR DEFAULTS CONDICIONAIS
    # ======================================================
    service_min = aplicar_default(args.service_min, 20.0)
    v_kmh = aplicar_default(args.v_kmh, 30.0)
    alpha_path = aplicar_default(args.alpha_path, 1.3)

    dias_uteis = aplicar_default(args.dias_uteis, 21)
    frequencia_visita = aplicar_default(args.frequencia_visita, 1)
    min_pdvs_rota = aplicar_default(args.min_pdvs_rota, 8)
    max_pdvs_rota = aplicar_default(args.max_pdvs_rota, 12)

    modo = aplicar_default(args.modo, "balanceado")
    modo_calculo = aplicar_default(args.modo_calculo, "frequencia")

    if min_pdvs_rota > max_pdvs_rota:
        print("❌ min_pdvs_rota não pode ser maior que max_pdvs_rota.")
        return

    # ======================================================
    # CRIAR routing_id
    # ======================================================
    routing_id = str(uuid.uuid4())
    clusterization_id = args.clusterization_id.strip()

    logger.info(f"🆕 Roteirização (modo={modo}) | routing_id={routing_id}")
    logger.info(
        f"Parâmetros: vel={v_kmh} km/h | α={alpha_path} | service={service_min} min | "
        f"dias_uteis={dias_uteis} | freq={frequencia_visita} | "
        f"min_pdvs={min_pdvs_rota} | max_pdvs={max_pdvs_rota}"
    )

    # ======================================================
    # DB services
    # ======================================================
    db_reader = SalesRoutingDatabaseReader()
    db_writer = SalesRoutingDatabaseWriter()

    # ======================================================
    # HISTÓRICO
    # ======================================================
    try:
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO historico_subcluster_jobs (
                        tenant_id, routing_id, clusterization_id,
                        descricao, criado_por, criado_em
                    )
                    VALUES (%s, %s, %s, %s, %s, NOW());
                """, (tenant_id, routing_id, clusterization_id, descricao, args.usuario))
                conn.commit()
        logger.success(f"Histórico criado (routing_id={routing_id})")
    except Exception as e:
        logger.error(f"Erro ao registrar histórico: {e}")
        return

    # ======================================================
    # CARREGAR RUN PELO clusterization_id (OBRIGATÓRIO)
    # ======================================================
    run = db_reader.get_run_by_clusterization_id(
        tenant_id=tenant_id,
        clusterization_id=clusterization_id
    )

    if not run:
        print(f"❌ Nenhum run encontrado para clusterization_id={clusterization_id}")
        return

    run_id = run["id"]
    print(f"Clusterização encontrada | run_id={run_id} | clusterization_id={clusterization_id}")



    clusters = db_reader.get_clusters(run_id)
    pdvs = db_reader.get_pdvs(run_id)

    # ======================================================
    # EXECUTAR SUBCLUSTERIZAÇÃO
    # ======================================================
    if modo == "fixo":
        resultados = gerar_subclusters_fixos(
            clusters=clusters,
            pdvs=pdvs,
            dias_uteis=dias_uteis,
            freq_padrao=frequencia_visita,
            v_kmh=v_kmh,
            service_min=service_min,
            alpha_path=alpha_path,
            aplicar_two_opt=args.twoopt,
            modo_calculo=modo_calculo,
        )
    elif modo == "balanceado":
        resultados = gerar_subclusters_balanceados(
            clusters=clusters,
            pdvs=pdvs,
            dias_uteis=dias_uteis,
            freq_padrao=frequencia_visita,
            v_kmh=v_kmh,
            service_min=service_min,
            alpha_path=alpha_path,
            aplicar_two_opt=args.twoopt,
            min_pdvs_rota=min_pdvs_rota,
            max_pdvs_rota=max_pdvs_rota,
            modo_calculo=modo_calculo,
        )
    else:
        resultados = gerar_subclusters_adaptativo(
            clusters=clusters,
            pdvs=pdvs,
            dias_uteis=dias_uteis,
            freq_padrao=frequencia_visita,
            v_kmh=v_kmh,
            service_min=service_min,
            alpha_path=alpha_path,
            aplicar_two_opt=args.twoopt,
        )
    # ======================================================
    # SALVAR RESULTADOS
    # ======================================================
    try:
        db_writer.salvar_operacional(
            resultados,
            tenant_id=tenant_id,
            run_id=run_id,
            routing_id=routing_id,
        )
        print("Resultados salvos com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar resultados: {e}")
        return

    print("\n🏁 Execução concluída.")


if __name__ == "__main__":
    main()

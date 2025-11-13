# ============================================================
# 📦 src/sales_clusterization/cli/run_cluster.py  (VERSÃO CORRIGIDA)
# ============================================================

import argparse
import uuid
from loguru import logger
from src.sales_clusterization.application.cluster_use_case import executar_clusterizacao


UF_VALIDAS = {
    "AC","AL","AP","AM","BA","CE","DF","ES","GO","MA",
    "MT","MS","MG","PA","PB","PR","PE","PI","RJ","RN",
    "RS","RO","RR","SC","SP","SE","TO",
}

# ------------------------------------------------------------
# 🔍 Valida UF
# ------------------------------------------------------------
def validar_uf(uf: str):
    uf = uf.strip().upper()
    if uf not in UF_VALIDAS:
        raise ValueError(
            f"UF inválida: '{uf}'. Deve ser uma das: {', '.join(sorted(UF_VALIDAS))}"
        )
    return uf

# ------------------------------------------------------------
# 🔍 Valida input_id (UUID)
# ------------------------------------------------------------
def validar_input_id(input_id: str):
    try:
        return str(uuid.UUID(input_id))
    except Exception:
        raise ValueError(f"input_id inválido: '{input_id}' — deve ser um UUID válido.")


# ANSI
COR_H1  = "\033[96m"
COR_OK  = "\033[92m"
COR_W   = "\033[93m"
COR_ER  = "\033[91m"
COR_RST = "\033[0m"


def main():
    parser = argparse.ArgumentParser(
        description="Executa clusterização de PDVs (SalesRouter / multi-tenant)"
    )

    # OBRIGATÓRIOS
    parser.add_argument("--tenant_id", type=int, required=True)
    parser.add_argument("--uf", required=True)
    parser.add_argument("--descricao", required=True)
    parser.add_argument("--input_id", required=True)

    # OPCIONAIS
    parser.add_argument("--cidade", required=False)

    parser.add_argument(
        "--algo",
        type=str,
        choices=["kmeans", "capacitated_sweep", "sweep", "dbscan"],
        help="Algoritmo: kmeans | dbscan | sweep | capacitated_sweep",
    )

    parser.add_argument("--dias_uteis", type=int, default=20)
    parser.add_argument("--freq", type=int, default=1)
    parser.add_argument("--workday", type=int, default=600)
    parser.add_argument("--routekm", type=float, default=200.0)
    parser.add_argument("--service", type=int, default=30)
    parser.add_argument("--vel", type=float, default=35.0)
    parser.add_argument("--alpha", type=float, default=1.3)
    parser.add_argument("--max_pdv_cluster", type=int, default=200)

    # ✔ max_iter mantém — mas não envia ao use_case se não existir
    parser.add_argument("--max_iter", type=int, default=10)

    parser.add_argument("--excluir_outliers", action="store_true")
    parser.add_argument("--clusterization_id", required=False)

    # ✔ padronizado para 3.0 (compatível com job/task)
    parser.add_argument("--z_thresh", type=float, default=3.0)

    args = parser.parse_args()

    # -----------------------------------------------------------
    # ✔ Alias sweep → capacitated_sweep
    # -----------------------------------------------------------
    if args.algo == "sweep":
        args.algo = "capacitated_sweep"

    # -----------------------------------------------------------
    # ✔ Valida UF
    # -----------------------------------------------------------
    try:
        uf = validar_uf(args.uf)
    except Exception as e:
        logger.error(f"{COR_ER}❌ Erro UF: {e}{COR_RST}")
        raise

    # -----------------------------------------------------------
    # ✔ Valida input_id
    # -----------------------------------------------------------
    try:
        input_id = validar_input_id(args.input_id)
    except Exception as e:
        logger.error(f"{COR_ER}❌ Erro input_id: {e}{COR_RST}")
        raise

    # -----------------------------------------------------------
    # ✔ Cidade tratada corretamente
    # -----------------------------------------------------------
    cidade = (
        args.cidade.strip()
        if args.cidade and args.cidade.strip().lower() not in ("none", "")
        else None
    )

    # -----------------------------------------------------------
    # ID único
    # -----------------------------------------------------------
    clusterization_id = args.clusterization_id or str(uuid.uuid4())

    # -----------------------------------------------------------
    # LOGS
    # -----------------------------------------------------------
    logger.info(f"{COR_H1}=============================================={COR_RST}")
    logger.info(f"{COR_H1}🚀 Iniciando job de clusterização (CLI){COR_RST}")
    logger.info(f"{COR_H1}=============================================={COR_RST}")

    logger.info(f"{COR_OK}🔑 tenant_id          = {args.tenant_id}{COR_RST}")
    logger.info(f"{COR_OK}📦 input_id           = {input_id}{COR_RST}")
    logger.info(f"{COR_OK}🗺️ UF                 = {uf}{COR_RST}")
    logger.info(f"{COR_OK}🏙️ cidade             = {cidade or 'ALL'}{COR_RST}")
    logger.info(f"{COR_OK}⚙️ algoritmo          = {args.algo}{COR_RST}")
    logger.info(f"{COR_OK}📝 descrição          = {args.descricao}{COR_RST}")
    logger.info(f"{COR_OK}🆔 clusterization_id  = {clusterization_id}{COR_RST}")

    logger.info(f"{COR_W}----- Parâmetros técnicos -----{COR_RST}")
    logger.info(f"{COR_W}🗓️ dias_uteis         = {args.dias_uteis}{COR_RST}")
    logger.info(f"{COR_W}🔁 freq               = {args.freq}{COR_RST}")
    logger.info(f"{COR_W}⏱️ jornada (min)      = {args.workday}{COR_RST}")
    logger.info(f"{COR_W}🛣️ rota máx (km)      = {args.routekm}{COR_RST}")
    logger.info(f"{COR_W}⚒ tempo serviço (min)= {args.service}{COR_RST}")
    logger.info(f"{COR_W}🚚 velocidade (km/h)  = {args.vel}{COR_RST}")
    logger.info(f"{COR_W}🔢 max_pdv_cluster    = {args.max_pdv_cluster}{COR_RST}")
    logger.info(f"{COR_W}🔧 max_iter           = {args.max_iter}{COR_RST}")
    logger.info(f"{COR_W}🧹 excluir_outliers   = {args.excluir_outliers}{COR_RST}")
    logger.info(f"{COR_W}📏 z_thresh           = {args.z_thresh}{COR_RST}")
    logger.info(f"{COR_H1}=============================================={COR_RST}")

    # -----------------------------------------------------------
    # Execução real
    # -----------------------------------------------------------
    result = executar_clusterizacao(
        tenant_id=args.tenant_id,
        uf=uf,
        cidade=cidade,
        algo=args.algo,
        dias_uteis=args.dias_uteis,
        freq=args.freq,
        workday_min=args.workday,
        route_km_max=args.routekm,
        service_min=args.service,
        v_kmh=args.vel,
        alpha_path=args.alpha,
        max_pdv_cluster=args.max_pdv_cluster,
        descricao=args.descricao,
        input_id=input_id,
        clusterization_id=clusterization_id,
        excluir_outliers=args.excluir_outliers,
        z_thresh=args.z_thresh,
        max_iter=args.max_iter,   # enviado se o use_case aceitar
    )

    # -----------------------------------------------------------
    # Resultado final
    # -----------------------------------------------------------
    print("\n=== RESULTADO FINAL ===")

    # Segurança — evita print quebrado
    for campo in ("clusterization_id", "run_id", "k_final", "n_pdvs"):
        val = result.get(campo, "N/A")
        print(f"{campo}: {val}")


if __name__ == "__main__":
    main()

#sales_router/src/sales_clusterization/cli/run_cluster.py

# ============================================================
# 📦 src/sales_clusterization/cli/run_cluster.py  (VERSÃO REAL)
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


def validar_uf(uf: str):
    uf = uf.strip().upper()
    if uf not in UF_VALIDAS:
        raise ValueError(f"UF inválida: {uf}")
    return uf


def validar_ufs(ufs_csv: str):
    """Aceita 'MG' ou 'MG,SP,RJ'. Retorna lista normalizada e única."""
    if not ufs_csv:
        raise ValueError("Pelo menos 1 UF é obrigatória.")
    parts = [p.strip().upper() for p in str(ufs_csv).split(",") if p.strip()]
    if not parts:
        raise ValueError("Pelo menos 1 UF válida é obrigatória.")
    invalidas = [p for p in parts if p not in UF_VALIDAS]
    if invalidas:
        raise ValueError(f"UF(s) inválida(s): {', '.join(invalidas)}")
    # Preserva ordem mas remove duplicatas
    seen = set()
    out = []
    for p in parts:
        if p not in seen:
            out.append(p)
            seen.add(p)
    return out


def validar_input_id(input_id: str):
    try:
        return str(uuid.UUID(input_id))
    except Exception:
        raise ValueError(f"input_id inválido: '{input_id}' — deve ser um UUID válido.")


def main():

    parser = argparse.ArgumentParser(
        description="Clusterização de PDVs (SalesRouter / multi-tenant)"
    )

    # OBRIGATÓRIOS
    parser.add_argument("--tenant_id", type=int, required=True)
    parser.add_argument("--uf", required=True)
    parser.add_argument("--descricao", required=True)
    parser.add_argument("--input_id", required=True)

    # OPCIONAL
    parser.add_argument("--cidade")

    # ✔ Somente o que existe
    parser.add_argument(
        "--algo",
        type=str,
        choices=["kmeans", "capacitated_sweep", "dense_subset"],
        default="kmeans",
        help="Algoritmo: kmeans, capacitated_sweep ou dense_subset"
    )


    # Parâmetros usados SOMENTE no KMeans
    parser.add_argument("--dias_uteis", type=int, default=20)
    parser.add_argument("--freq", type=int, default=1)
    parser.add_argument("--workday", type=int, default=600)
    parser.add_argument("--routekm", type=float, default=200.0)
    parser.add_argument("--service", type=int, default=30)
    parser.add_argument("--vel", type=float, default=35.0)
    # Fator de correção haversine → distância real por estrada.
    # Aplicado no refinador operacional pra estimativa de tempo/dist
    # ficar próxima do que o OSRM devolve depois na roteirização.
    # Não exposto na UI; default 1.4.
    parser.add_argument("--alpha", type=float, default=1.4)

    # Teto de PDVs por setor. Sem default no backend — o frontend envia o
    # valor. Quando o modo precisa dele e não vier, o use case acusa erro.
    parser.add_argument("--max_pdv_cluster", type=int, default=None)
    # Piso opcional de PDVs por setor (banda). Sem ele, não há fusão de
    # setores pequenos — a clusterização roda normalmente.
    parser.add_argument("--min_pdv_cluster", type=int, default=None)
    parser.add_argument("--max_iter", type=int, default=10)
    # operacional: kmeans_balanceado + refinador (default, atual)
    # capacidade: só kmeans_balanceado (respeita só max_pdv_cluster)
    # fixo:       roda KMeans com K=k_forcado, sem balancear nem refinar
    parser.add_argument(
        "--modo_refinamento",
        choices=["operacional", "capacidade", "fixo"],
        default="operacional",
    )

    parser.add_argument("--excluir_outliers", action="store_true")
    parser.add_argument("--clusterization_id")
    parser.add_argument("--z_thresh", type=float, default=3.0)
    # K alvo para modo_refinamento="fixo" — usuário define exatamente
    # quantos setores quer. Default None = não usa.
    parser.add_argument("--k_forcado", type=int, default=None)

    args = parser.parse_args()

    # ============================================================
    # Validações
    # ============================================================
    # --uf aceita CSV ("MG,SP,RJ"). Internamente vira lista de strings.
    ufs = validar_ufs(args.uf)
    # Pra compat com o resto do código, passa string única quando só 1 UF;
    # senão a lista inteira (use_case e carregar_pdvs já lidam com ambos).
    uf = ufs[0] if len(ufs) == 1 else ufs
    input_id = validar_input_id(args.input_id)

    cidade = (
        args.cidade.strip()
        if args.cidade and args.cidade.strip().lower() not in ("none", "")
        else None
    )
    # Cidade só faz sentido com 1 UF — ignora se múltiplas UFs.
    if isinstance(uf, list) and cidade:
        logger.warning(
            f"⚠️ Cidade '{cidade}' ignorada — múltiplas UFs ({len(ufs)})."
        )
        cidade = None

    clusterization_id = args.clusterization_id or str(uuid.uuid4())

    # ============================================================
    # Logs
    # ============================================================
    logger.info("==============================================")
    logger.info("🚀 Iniciando clusterização via CLI")
    logger.info("==============================================")
    logger.info(f"🔑 tenant_id          = {args.tenant_id}")
    logger.info(f"📦 input_id           = {input_id}")
    logger.info(
        f"🗺️ UF                 = "
        f"{','.join(uf) if isinstance(uf, list) else uf}"
    )
    logger.info(f"🏙️ cidade             = {cidade or 'ALL'}")
    logger.info(f"⚙️ algoritmo          = {args.algo}")
    logger.info(f"📝 descrição          = {args.descricao}")
    logger.info(f"🆔 clusterization_id  = {clusterization_id}")

    logger.info("----- Parâmetros -----")

    if args.algo == "kmeans":
        logger.info(f"🗓️ dias_uteis         = {args.dias_uteis}")
        logger.info(f"🔁 freq               = {args.freq}")
        logger.info(f"⏱️ jornada (min)      = {args.workday}")
        logger.info(f"🛣️ rota máx (km)      = {args.routekm}")
        logger.info(f"⚒ tempo serviço (min)= {args.service}")
        logger.info(f"🚚 velocidade (km/h)  = {args.vel}")

    logger.info(f"🔢 max_pdv_cluster    = {args.max_pdv_cluster}")
    logger.info(f"🔢 min_pdv_cluster    = {args.min_pdv_cluster}")
    logger.info(f"🎛️ modo_refinamento   = {args.modo_refinamento}")
    if args.k_forcado:
        logger.info(f"🎯 k_forcado          = {args.k_forcado}")
    logger.info(f"🔧 max_iter           = {args.max_iter}")
    logger.info(f"🧹 excluir_outliers   = {args.excluir_outliers}")
    logger.info(f"📏 z_thresh           = {args.z_thresh}")

    # ============================================================
    # Execução
    # ============================================================
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
        min_pdv_cluster=args.min_pdv_cluster,
        descricao=args.descricao,
        input_id=input_id,
        clusterization_id=clusterization_id,
        excluir_outliers=args.excluir_outliers,
        z_thresh=args.z_thresh,
        max_iter=args.max_iter,
        modo_refinamento=args.modo_refinamento,
        k_forcado=args.k_forcado,
    )

    print("\n=== RESULTADO FINAL ===")
    for campo in ("clusterization_id", "run_id", "k_final", "n_pdvs"):
        print(f"{campo}: {result.get(campo, 'N/A')}")


if __name__ == "__main__":
    main()

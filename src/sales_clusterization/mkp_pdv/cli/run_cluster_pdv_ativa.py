#sales_router/src/sales_clusterization/mkp_pdv/cli/run_cluster_pdv_ativa.py

# ============================================================
# 📦 CLI — Clusterização MKP baseada em PDVs (Modo Ativo)
# ============================================================

import argparse
from loguru import logger
from ..application.cluster_pdv_ativa_use_case import ClusterPDVAtivaUseCase


def main():
    parser = argparse.ArgumentParser(
        description="Executa clusterização MKP PDV (ativa)"
    )

    # ----------------------------
    # Parâmetros obrigatórios
    # ----------------------------
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--uf", type=str, required=True, help="UF")
    parser.add_argument("--cidade", type=str, help="Cidade opcional")
    parser.add_argument("--input_id", type=str, required=True, help="Input ID (tabela PDVs)")
    parser.add_argument("--centros_csv", type=str, required=True, help="Caminho para CSV dos centros")
    parser.add_argument("--descricao", type=str, required=True, help="Descrição da clusterização")

    args = parser.parse_args()

    logger.remove()
    logger.add(lambda m: print(m, end=""),
               colorize=True,
               format="<green>{time:HH:mm:ss}</green> | <level>{message}</level>")

    logger.info("🚀 Iniciando clusterização MKP PDV (ativa)")

    # ----------------------------
    # Execução
    # ----------------------------
    uc = ClusterPDVAtivaUseCase()

    clusterization_id = uc.executar(
        tenant_id=args.tenant,
        uf=args.uf.upper(),
        cidade=args.cidade,
        input_id=args.input_id,
        centros_csv=args.centros_csv,
        descricao=args.descricao
    )

    logger.success(
        f"🏁 Clusterização MKP PDV Ativa finalizada | clusterization_id={clusterization_id}"
    )


if __name__ == "__main__":
    main()

#sales_router/src/sales_clusterization/mkp_pdv/cli/run_cluster_pdv_balanceada.py

import argparse
from loguru import logger
from ..application.cluster_pdv_balanceada_use_case import ClusterPDVBalanceadaUseCase

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--tenant", type=int, required=True)
    p.add_argument("--uf", type=str, required=True)
    p.add_argument("--cidade", type=str)
    p.add_argument("--input_id", type=str, required=True)
    p.add_argument("--centros_csv", type=str, required=True)
    p.add_argument("--descricao", type=str, required=True)
    p.add_argument("--min_pdv", type=int, required=True)
    p.add_argument("--max_pdv", type=int, required=True)
    args = p.parse_args()

    uc = ClusterPDVBalanceadaUseCase()
    cid = uc.executar(
        args.tenant, args.uf, args.cidade,
        args.input_id, args.centros_csv, args.descricao,
        args.min_pdv, args.max_pdv
    )

    logger.success(f"🏁 Clusterização PDV balanceada finalizada | id={cid}")

if __name__ == "__main__":
    main()

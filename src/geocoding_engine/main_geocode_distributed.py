#sales_router/src/geocoding_engine/main_geocode_distributed.py

import argparse
import pandas as pd
from rq import get_current_job

from geocoding_engine.application.geocode_spreadsheet_use_case import GeocodeSpreadsheetUseCase


# =========================================================
# 🔥 PROGRESSO DIRETO NO REDIS (SEM STDOUT)
# =========================================================
def update_progress(pct, step):

    job = get_current_job()

    if job:
        job.meta["progress"] = int(pct)
        job.meta["step"] = step
        job.save_meta()


# =========================================================
# MAIN
# =========================================================
def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--arquivo", required=True)
    parser.add_argument("--saida", required=True)

    args = parser.parse_args()

    # -----------------------------------------------------
    # LEITURA
    # -----------------------------------------------------
    update_progress(5, "Lendo arquivo")

    df = pd.read_excel(args.arquivo)

    # -----------------------------------------------------
    # PROCESSAMENTO
    # -----------------------------------------------------
    update_progress(20, "Geocodificando")

    uc = GeocodeSpreadsheetUseCase()
    excel_buffer, stats = uc.execute(df)

    # -----------------------------------------------------
    # SALVAR
    # -----------------------------------------------------
    update_progress(90, "Salvando resultado")

    with open(args.saida, "wb") as f:
        f.write(excel_buffer.getvalue())

    # -----------------------------------------------------
    # FINALIZAÇÃO
    # -----------------------------------------------------
    update_progress(100, "Concluído")

    # 🔥 mantém resultado no job (sem stdout)
    job = get_current_job()
    if job:
        job.meta["result"] = stats
        job.save_meta()


if __name__ == "__main__":
    main()
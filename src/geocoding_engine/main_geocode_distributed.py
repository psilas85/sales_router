#sales_router/src/geocoding_engine/main_geocode_distributed.py

import argparse
import pandas as pd
from rq import get_current_job
from loguru import logger
import traceback

from geocoding_engine.application.geocode_spreadsheet_use_case import GeocodeSpreadsheetUseCase


# =========================================================
# 🔥 PROGRESSO REDIS
# =========================================================
def update_progress(pct, step):

    try:
        job = get_current_job()

        if job:
            job.meta["progress"] = int(pct)
            job.meta["step"] = step
            job.save_meta()

    except Exception as e:
        logger.warning(f"[PROGRESS][ERRO] {e}")


# =========================================================
# MAIN
# =========================================================
def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--arquivo", required=True)
    parser.add_argument("--saida", required=True)

    args = parser.parse_args()

    job = get_current_job()

    try:
        # -----------------------------------------------------
        # LEITURA
        # -----------------------------------------------------
        update_progress(5, "Lendo seu arquivo")

        logger.info(f"[JOB] lendo arquivo: {args.arquivo}")

        df = pd.read_excel(args.arquivo)

        if df.empty:
            raise Exception("Arquivo vazio")

        logger.info(f"[JOB] linhas carregadas: {len(df)}")

        # -----------------------------------------------------
        # PROCESSAMENTO
        # -----------------------------------------------------
        update_progress(20, "Localizando enderecos")

        uc = GeocodeSpreadsheetUseCase()

        excel_buffer, stats = uc.execute(
            df,
            progress_callback=update_progress  # ✅ CORRETO
        )

        logger.info(f"[JOB] processamento concluído: {stats}")

        # -----------------------------------------------------
        # SALVAR
        # -----------------------------------------------------
        update_progress(90, "Preparando sua entrega")

        try:
            with open(args.saida, "wb") as f:
                f.write(excel_buffer.getvalue())

            logger.info(f"[JOB] arquivo salvo: {args.saida}")

        except Exception as e:
            logger.error(f"[JOB][ERRO_SALVAR] {e}")
            raise

        # -----------------------------------------------------
        # FINALIZAÇÃO
        # -----------------------------------------------------
        update_progress(100, "Processamento concluido")

        if job:
            job.meta["result"] = stats
            job.meta["status"] = "done"
            job.save_meta()

    except Exception as e:

        erro = str(e)
        stack = traceback.format_exc()

        logger.error(f"[JOB][ERRO] {erro}")
        logger.error(stack)

        update_progress(100, "Nao foi possivel concluir")

        if job:
            job.meta["status"] = "failed"
            job.meta["error"] = erro
            job.meta["stack"] = stack
            job.save_meta()

        raise


if __name__ == "__main__":
    main()
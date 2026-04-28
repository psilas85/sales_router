#sales_router/src/geocoding_engine/main_geocode_spreadsheet.py

import argparse
import pandas as pd
import json
import sys

from geocoding_engine.application.geocode_spreadsheet_use_case import GeocodeSpreadsheetUseCase


def emit_progress(pct, step):

    print(json.dumps({
        "event": "progress",
        "pct": pct,
        "step": step
    }))

    sys.stdout.flush()


def emit_final(obj):

    print(json.dumps(obj))
    sys.stdout.flush()


def main():

    parser = argparse.ArgumentParser()

    parser.add_argument("--arquivo", required=True)
    parser.add_argument("--saida", required=True)

    args = parser.parse_args()

    emit_progress(5, "Lendo seu arquivo")

    df = pd.read_excel(args.arquivo)

    emit_progress(20, "Localizando enderecos")

    uc = GeocodeSpreadsheetUseCase()

    excel_buffer, stats = uc.execute(df)

    with open(args.saida, "wb") as f:
        f.write(excel_buffer.getvalue())

    emit_progress(90, "Preparando sua entrega")

    emit_final({
        "status": "done",
        "stats": stats
    })


if __name__ == "__main__":
    main()
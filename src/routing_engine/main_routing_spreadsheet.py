# sales_router/src/routing_engine/main_routing_spreadsheet.py

import sys
import json
import time
import shutil
import os

from routing_engine.application.route_spreadsheet_use_case import RouteSpreadsheetUseCase


def log_progress(pct, step):
    print(json.dumps({
        "event": "progress",
        "pct": pct,
        "step": step
    }), flush=True)


if __name__ == "__main__":

    file_path = sys.argv[1]
    output_file = sys.argv[2]

    start = time.time()

    log_progress(5, "Carregando arquivo")

    with open(file_path, "rb") as f:
        file_bytes = f.read()

    log_progress(10, "Inicializando roteirização")

    uc = RouteSpreadsheetUseCase()

    log_progress(20, "Processando dados")

    result = uc.execute(
        file_bytes=file_bytes,
        filename="input.xlsx"
    )

    # =========================================================
    # 🔥 GARANTE OUTPUT NO CAMINHO CERTO
    # =========================================================
    if isinstance(result, str) and os.path.exists(result):
        if result != output_file:
            shutil.copy(result, output_file)

    elif isinstance(result, dict) and result.get("output"):
        origem = result["output"]
        if origem and os.path.exists(origem) and origem != output_file:
            shutil.copy(origem, output_file)

    log_progress(90, "Finalizando")

    elapsed = int((time.time() - start) * 1000)

    print(json.dumps({
        "status": "done",
        "output": result,
        "output_file": output_file,
        "tempo_execucao_ms": elapsed
    }), flush=True)
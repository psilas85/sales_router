#sales_router/src/routing_engine/tests/test_routing.py

import os

from routing_engine.application.route_spreadsheet_use_case import RouteSpreadsheetUseCase


if __name__ == "__main__":

    file_path = "/app/data/input/geocode_result.xlsx"

    if not os.path.exists(file_path):
        raise Exception(f"Arquivo não encontrado: {file_path}")

    with open(file_path, "rb") as f:
        file_bytes = f.read()

    uc = RouteSpreadsheetUseCase()

    output = uc.execute(
        file_bytes=file_bytes,
        filename="geocode_result.xlsx",
        tenant_id=1
    )

    print("Arquivo gerado:", output)
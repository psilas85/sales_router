#sales_router/src/routing_engine/services/spreadsheet_loader_service.py

from __future__ import annotations

import io
import pandas as pd


class SpreadsheetLoaderService:
    def load(self, file_bytes: bytes, filename: str) -> pd.DataFrame:
        if not filename:
            raise ValueError("Nome do arquivo não informado.")

        filename_lower = filename.lower()

        if filename_lower.endswith(".xlsx"):
            return pd.read_excel(io.BytesIO(file_bytes))

        if filename_lower.endswith(".csv"):
            try:
                return pd.read_csv(io.BytesIO(file_bytes), sep=",")
            except Exception:
                return pd.read_csv(io.BytesIO(file_bytes), sep=";")

        raise ValueError("Formato de arquivo não suportado. Use .xlsx ou .csv")
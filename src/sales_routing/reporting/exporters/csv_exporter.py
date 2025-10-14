#sales_router/src/sales_routing/reporting/exporters/csv_exporter.py

import os
import pandas as pd
from loguru import logger
from datetime import datetime


class CSVExporter:
    """
    Exporta DataFrames em formato CSV com formatação limpa e compatível com Excel.
    """

    @staticmethod
    def export(df: pd.DataFrame, output_dir: str = "output/reports", nome_base: str = "rotas_resumo"):
        if df.empty:
            logger.warning("⚠️ DataFrame vazio — nada a exportar.")
            return None

        # =========================================================
        # Detecta se o argumento é um arquivo ou diretório
        # =========================================================
        if output_dir.endswith(".csv"):
            output_path = output_dir
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
        else:
            os.makedirs(output_dir, exist_ok=True)
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_path = os.path.join(output_dir, f"{nome_base}_{timestamp}.csv")

        # =========================================================
        # Ajusta colunas numéricas — arredonda e formata
        # =========================================================
        num_cols = df.select_dtypes(include=["float", "int"]).columns
        df[num_cols] = df[num_cols].apply(pd.to_numeric, errors="coerce")
        df[num_cols] = df[num_cols].round(2)

        # =========================================================
        # Exporta com formatação limpa (sem notação científica)
        # =========================================================
        df.to_csv(
            output_path,
            index=False,
            sep=";",
            encoding="utf-8-sig",
            float_format="%.2f"
        )

        logger.success(f"✅ Relatório CSV salvo em {output_path}")
        return output_path

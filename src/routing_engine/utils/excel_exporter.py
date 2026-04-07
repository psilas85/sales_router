#sales_router/src/routing_engine/utils/excel_exporter.py

from __future__ import annotations

import os
import pandas as pd


class ExcelExporter:
    def export(
        self,
        df_detalhe: pd.DataFrame,
        df_resumo: pd.DataFrame,
        df_metricas: pd.DataFrame,
        output_dir: str,
        filename: str,
        df_invalidos: pd.DataFrame | None = None,  # 🔥 NOVO
    ) -> str:

        os.makedirs(output_dir, exist_ok=True)
        file_path = os.path.join(output_dir, filename)

        with pd.ExcelWriter(file_path, engine="openpyxl") as writer:

            # ===============================
            # ABA 1 - DETALHE
            # ===============================
            df_detalhe.to_excel(
                writer,
                sheet_name="roteirizacao_detalhe",
                index=False
            )
            self._auto_adjust(writer, "roteirizacao_detalhe", df_detalhe)

            # ===============================
            # ABA 2 - RESUMO
            # ===============================
            df_resumo.to_excel(
                writer,
                sheet_name="roteirizacao_resumo",
                index=False
            )
            self._auto_adjust(writer, "roteirizacao_resumo", df_resumo)

            # ===============================
            # ABA 3 - METRICAS
            # ===============================
            df_metricas.to_excel(
                writer,
                sheet_name="roteirizacao_metricas",
                index=False
            )
            self._auto_adjust(writer, "roteirizacao_metricas", df_metricas)

            # ===============================
            # 🔥 ABA 4 - INVALIDOS
            # ===============================
            if df_invalidos is not None and not df_invalidos.empty:

                df_invalidos.to_excel(
                    writer,
                    sheet_name="invalidos",
                    index=False
                )

                self._auto_adjust(writer, "invalidos", df_invalidos)

        return file_path

    def _auto_adjust(self, writer, sheet_name: str, df: pd.DataFrame):
        ws = writer.sheets[sheet_name]

        for idx, col in enumerate(df.columns, start=1):
            max_len = max(
                [len(str(col))]
                + [
                    len(str(v))
                    for v in df[col]
                    .fillna("")
                    .astype(str)
                    .head(1000)
                    .tolist()
                ]
            )
            ws.column_dimensions[self._excel_col(idx)].width = min(max(max_len + 2, 12), 60)

    def _excel_col(self, n: int) -> str:
        result = ""
        while n:
            n, rem = divmod(n - 1, 26)
            result = chr(65 + rem) + result
        return result
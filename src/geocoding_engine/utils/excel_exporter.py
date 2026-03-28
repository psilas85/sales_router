#sales_router/src/geocoding_engine/utils/excel_exporter.py

import io
import pandas as pd


class ExcelExporter:

    @staticmethod
    def gerar_excel(df_validos, df_invalidos):

        buffer = io.BytesIO()

        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:

            if not df_validos.empty:
                df_validos.to_excel(
                    writer,
                    sheet_name="geocodificados",
                    index=False
                )

            if not df_invalidos.empty:
                df_invalidos.to_excel(
                    writer,
                    sheet_name="invalidos",
                    index=False
                )

        buffer.seek(0)

        return buffer
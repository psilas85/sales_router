# sales_router/src/pdv_preprocessing/utils/file_utils.py

import os
import logging
import pandas as pd


def detectar_separador(path: str) -> str:
    """Detecta automaticamente o separador do CSV."""
    with open(path, "r", encoding="utf-8-sig") as f:
        linha = f.readline()
        return ";" if ";" in linha else ","


def salvar_invalidos(df_invalidos: pd.DataFrame, pasta_base: str, input_id: str) -> str | None:
    try:
        if df_invalidos is None or df_invalidos.empty:
            return None

        pasta_invalidos = os.path.join(pasta_base, "invalidos")
        os.makedirs(pasta_invalidos, exist_ok=True)

        nome_arquivo = f"pdvs_invalidos_{input_id}.xlsx"
        caminho_saida = os.path.join(pasta_invalidos, nome_arquivo)

        df_invalidos.to_excel(
            caminho_saida,
            index=False,
            engine="openpyxl"
        )

        logging.warning(f"⚠️ {len(df_invalidos)} inválidos salvos em: {caminho_saida}")
        return caminho_saida

    except Exception as e:
        logging.error(f"❌ Erro ao salvar inválidos: {e}", exc_info=True)
        return None


#sales_router/src/pdv_preprocessing/application/mkp_preprocessing_use_case.py

# ============================================================
# 📦 src/pdv_preprocessing/application/mkp_preprocessing_use_case.py
# ============================================================

import pandas as pd
import logging
import unicodedata

from pdv_preprocessing.domain.mkp_validation_service import MKPValidationService
from pdv_preprocessing.domain.utils_geo import cep_invalido


class MKPPreprocessingUseCase:

    def __init__(self, reader, writer, tenant_id, input_id=None, descricao=None):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.input_id = input_id
        self.descricao = descricao

        self.validator = MKPValidationService()

    # ------------------------------------------------------------
    def normalizar_colunas(self, df):
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .map(lambda x: unicodedata.normalize("NFKD", x)
                 .encode("ascii", errors="ignore")
                 .decode("utf-8"))
        )
        return df

    # ------------------------------------------------------------
    def limpar_valores(self, df):
        # Cidade / UF / bairro
        for col in ["cidade", "uf", "bairro"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .replace({"nan": "", "None": ""})
                )

        if "uf" in df.columns:
            df["uf"] = df["uf"].str.upper().str.strip()

        if "cidade" in df.columns:
            df["cidade"] = (
                df["cidade"]
                .astype(str)
                .apply(lambda x: unicodedata.normalize("NFKD", x)
                       .encode("ascii", errors="ignore")
                       .decode("utf-8")
                       .upper()
                       .strip())
            )

        for campo in ["clientes_total", "clientes_target"]:
            if campo in df.columns:
                df[campo] = pd.to_numeric(df[campo], errors="coerce")

        return df

    # ------------------------------------------------------------
    def execute_df(self, df: pd.DataFrame):
        """
        🔥 Limpo, puro, SES (Single Entry Step):
        - Normaliza colunas
        - Limpa valores
        - Valida com regras MKP
        - Retorna df_validos, df_invalidos, quantidade_validos
        """
        df = self.normalizar_colunas(df)
        df = self.limpar_valores(df)

        # Garantir coluna bairro
        if "bairro" not in df.columns:
            logging.warning("⚠️ Input MKP sem coluna 'bairro'. Criando coluna vazia.")
            df["bairro"] = ""

        validos, invalidos = self.validator.validar_dados(df)

        # CEP inválido imediato → vai para inválidos
        ceps_invalidos = validos[validos["cep"].apply(cep_invalido)]
        if not ceps_invalidos.empty:
            ceps_invalidos = ceps_invalidos.assign(
                lat=None,
                lon=None,
                status_geolocalizacao="cep_invalido",
                motivo_invalidade_geo="cep_invalido"
            )
            invalidos = pd.concat([invalidos, ceps_invalidos], ignore_index=True)

        validos = validos[~validos["cep"].apply(cep_invalido)].copy()

        # Metadados básicos (sem banco!)
        validos["tenant_id"] = self.tenant_id
        validos["input_id"] = self.input_id
        validos["descricao"] = self.descricao

        # lat/lon inicializados sempre como None
        validos["lat"] = None
        validos["lon"] = None
        validos["status_geolocalizacao"] = None

        return validos.reset_index(drop=True), invalidos.reset_index(drop=True), len(validos)

# src/pdv_preprocessing/domain/pdv_validation_service.py

import pandas as pd
import numpy as np
import re
import logging

class PDVValidationService:

    @staticmethod
    def limpar_cnpj(cnpj: str) -> str:
        if pd.isna(cnpj):
            return None
        return re.sub(r"[^0-9]", "", str(cnpj))

    @staticmethod
    def limpar_cep(cep: str) -> str:
        if pd.isna(cep):
            return None
        cep = re.sub(r"[^0-9]", "", str(cep))
        return cep.zfill(8) if cep else None

    def validar_dados(self, df: pd.DataFrame) -> pd.DataFrame:
        campos_obrigatorios = ["cnpj", "logradouro", "numero", "bairro", "cidade", "uf", "cep"]
        registros_invalidos = df[df[campos_obrigatorios].isna().any(axis=1)].copy()

        if not registros_invalidos.empty:
            registros_invalidos["motivo_invalidade"] = registros_invalidos.apply(
                lambda row: ", ".join([c for c in campos_obrigatorios if pd.isna(row[c])]), axis=1
            )
            logging.warning(f"⚠️ {len(registros_invalidos)} registro(s) inválido(s) detectado(s).")

        df_validos = df.dropna(subset=campos_obrigatorios).copy()
        return df_validos, registros_invalidos

#sales_router/src/pdv_preprocessing/domain/mkp_validation_service.py

import pandas as pd
import numpy as np
import logging

class MKPValidationService:
    """
    Serviço de validação dos dados de marketplace agregados por CEP.
    - Valida presença de colunas obrigatórias.
    - Garante que não haja valores nulos ou vazios.
    - Ajusta tipos de clientes_total e clientes_target.
    """

    CAMPOS_OBRIGATORIOS = ["cidade", "uf", "bairro", "cep", "clientes_total"]

    def validar_dados(self, df: pd.DataFrame, tenant_id: int = None, input_id: str = None):
        df[self.CAMPOS_OBRIGATORIOS] = df[self.CAMPOS_OBRIGATORIOS].replace("", np.nan)

        invalidos = df[df[self.CAMPOS_OBRIGATORIOS].isna().any(axis=1)].copy()
        validos = df.dropna(subset=self.CAMPOS_OBRIGATORIOS).copy()

        if not invalidos.empty:
            invalidos["motivo_invalidade"] = invalidos.apply(
                lambda r: ", ".join([c for c in self.CAMPOS_OBRIGATORIOS if pd.isna(r[c])]),
                axis=1
            )
            logging.warning(f"⚠️ {len(invalidos)} registros com campos obrigatórios faltando.")

        # Conversão de tipos
        validos["clientes_total"] = pd.to_numeric(validos["clientes_total"], errors="coerce").fillna(0).astype(int)
        if "clientes_target" in validos.columns:
            validos["clientes_target"] = pd.to_numeric(validos["clientes_target"], errors="coerce").fillna(0).astype(int)
        else:
            validos["clientes_target"] = 0

        return validos, invalidos

# ============================================================
# 📦 src/pdv_preprocessing/domain/mkp_validation_service.py
# ============================================================

import pandas as pd
import numpy as np
import logging
from pdv_preprocessing.config.cep_bounds import CEP_BOUNDS


class MKPValidationService:
    """
    Validação de dados de Marketplace (MKP)

    PRINCÍPIOS:
    - Validação é linha a linha
    - CEP repetido é permitido
    - NÃO gera identidade (mkp_id)
    - NÃO agrega dados
    """

    CAMPOS_OBRIGATORIOS = ["cidade", "uf", "cep", "clientes_total"]

    # ---------------------------------------------------------
    @staticmethod
    def _normalizar_cep(valor):
        if valor is None or pd.isna(valor):
            return None

        valor = str(valor).strip()

        # Remove .0 de Excel
        if valor.endswith(".0"):
            valor = valor[:-2]

        # Trata notação científica
        if "e" in valor.lower():
            try:
                valor = f"{int(float(valor))}"
            except Exception:
                return None

        # Mantém apenas dígitos
        valor = "".join(ch for ch in valor if ch.isdigit())

        if len(valor) < 5 or len(valor) > 8:
            return None

        return valor.zfill(8)

    # ---------------------------------------------------------
    @staticmethod
    def _cep_compat_uf(cep: str, uf: str) -> bool:
        if not cep or not uf:
            return False

        uf = uf.strip().upper()
        if uf not in CEP_BOUNDS:
            return False

        faixa_min, faixa_max = CEP_BOUNDS[uf]
        return faixa_min <= cep <= faixa_max

    # ---------------------------------------------------------
    def validar_dados(self, df: pd.DataFrame):
        """
        Retorna:
        - validos
        - invalidos

        Ambos preservam:
        - ordem original
        - granularidade de linha
        """

        if df is None or df.empty:
            return (
                pd.DataFrame(),
                pd.DataFrame()
            )

        df = df.copy()
        df["_linha_origem"] = range(len(df))  # rastreabilidade

        # -----------------------------------------------------
        # Garante colunas obrigatórias
        # -----------------------------------------------------
        for col in self.CAMPOS_OBRIGATORIOS:
            if col not in df.columns:
                df[col] = np.nan

        # -----------------------------------------------------
        # Normalização textual
        # -----------------------------------------------------
        for col in ["cidade", "uf", "bairro"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .replace({"nan": "", "None": ""})
                    .str.upper()
                )

        # -----------------------------------------------------
        # Normalização de CEP
        # -----------------------------------------------------
        df["cep"] = df["cep"].apply(self._normalizar_cep)

        # -----------------------------------------------------
        # Marca obrigatórios vazios
        # -----------------------------------------------------
        df[self.CAMPOS_OBRIGATORIOS] = (
            df[self.CAMPOS_OBRIGATORIOS]
            .replace("", np.nan)
        )

        invalidos = df[df[self.CAMPOS_OBRIGATORIOS].isna().any(axis=1)].copy()
        validos = df.dropna(subset=self.CAMPOS_OBRIGATORIOS).copy()

        if not invalidos.empty:
            invalidos["motivo_invalidade"] = invalidos.apply(
                lambda r: ", ".join(
                    [c for c in self.CAMPOS_OBRIGATORIOS if pd.isna(r[c])]
                ),
                axis=1
            )

        # -----------------------------------------------------
        # Validação CEP × UF (somente nos válidos até aqui)
        # -----------------------------------------------------
        validos["compat_uf_cep"] = [
            self._cep_compat_uf(cep, uf)
            for cep, uf in zip(validos["cep"], validos["uf"])
        ]

        invalidos_cep = validos[validos["compat_uf_cep"] == False].copy()
        if not invalidos_cep.empty:
            invalidos_cep["motivo_invalidade"] = "CEP não compatível com UF"

        validos = validos[validos["compat_uf_cep"] == True].copy()

        # -----------------------------------------------------
        # Tipagem numérica
        # -----------------------------------------------------
        validos["clientes_total"] = (
            pd.to_numeric(validos["clientes_total"], errors="coerce")
            .fillna(0)
            .astype(int)
        )

        if "clientes_target" in validos.columns:
            validos["clientes_target"] = (
                pd.to_numeric(validos["clientes_target"], errors="coerce")
                .fillna(0)
                .astype(int)
            )
        else:
            validos["clientes_target"] = 0

        # -----------------------------------------------------
        # Consolidação dos inválidos
        # -----------------------------------------------------
        invalidos = pd.concat(
            [invalidos, invalidos_cep],
            ignore_index=True
        )

        # -----------------------------------------------------
        # Limpeza de colunas auxiliares
        # -----------------------------------------------------
        for col in ["compat_uf_cep"]:
            if col in validos.columns:
                validos = validos.drop(columns=[col])
            if col in invalidos.columns:
                invalidos = invalidos.drop(columns=[col])

        return (
            validos.reset_index(drop=True),
            invalidos.reset_index(drop=True)
        )

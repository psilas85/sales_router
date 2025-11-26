#sales_router/src/pdv_preprocessing/domain/mkp_validation_service.py

import pandas as pd
import numpy as np
import logging
from pdv_preprocessing.config.cep_bounds import CEP_BOUNDS


class MKPValidationService:
    """
    Validação robusta:
    ✔ XLSX seguro (zeros preservados)
    ✔ Campos obrigatórios
    ✔ UF × CEP
    ✔ Conversões numéricas seguras
    ✔ Motivos de invalidez
    """

    CAMPOS_OBRIGATORIOS = ["cidade", "uf", "cep", "clientes_total"]

    # ------------------------------------------------------------
    @staticmethod
    def _normalizar_cep(valor):
        """Remove lixo, preserva zeros, resolve float e notação científica."""
        if valor is None or pd.isna(valor):
            return None

        valor = str(valor).strip()

        # Remove .0 do Excel
        if valor.endswith(".0"):
            valor = valor[:-2]

        # Remove notação científica — Excel salva assim
        if "e" in valor.lower():
            try:
                valor = f"{int(float(valor))}"
            except:
                return None

        # Mantém só números
        valor = "".join(ch for ch in valor if ch.isdigit())
        if len(valor) < 5 or len(valor) > 8:
            return None

        return valor.zfill(8)

    # ------------------------------------------------------------
    @staticmethod
    def _cep_compat_uf(cep: str, uf: str) -> bool:
        if not cep or not uf:
            return False
        uf = uf.strip().upper()
        if uf not in CEP_BOUNDS:
            return False
        faixa_min, faixa_max = CEP_BOUNDS[uf]
        return faixa_min <= cep <= faixa_max

    # ============================================================
    def validar_dados(self, df: pd.DataFrame, tenant_id: int = None, input_id: str = None):

        df = df.copy()

        # ============================================================
        # 0) Garantir que todas as colunas existam
        # ============================================================
        for col in self.CAMPOS_OBRIGATORIOS:
            if col not in df.columns:
                df[col] = np.nan  # coluna faltante → todos inválidos

        # ============================================================
        # 1) Normalização prévia (antes de validar obrigatórios)
        # ============================================================
        # Remover espaços e normalizar textos
        for col in ["cidade", "uf", "bairro"]:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.strip()
                    .replace({"nan": "", "None": ""})
                    .str.upper()
                )

        # CEP sempre tratado como texto
        df["cep"] = df["cep"].apply(self._normalizar_cep)

        # ============================================================
        # 2) Validação de obrigatórios
        # ============================================================
        # Evita erro "float cannot convert string"
        df[self.CAMPOS_OBRIGATORIOS] = df[self.CAMPOS_OBRIGATORIOS].replace("", np.nan)

        invalidos = df[df[self.CAMPOS_OBRIGATORIOS].isna().any(axis=1)].copy()
        validos = df.dropna(subset=self.CAMPOS_OBRIGATORIOS).copy()

        if len(invalidos) > 0:
            invalidos["motivo_invalidade"] = invalidos.apply(
                lambda r: ", ".join(
                    [c for c in self.CAMPOS_OBRIGATORIOS if pd.isna(r[c])]
                ),
                axis=1
            )
            logging.warning(f"⚠️ {len(invalidos)} inválidos — campos obrigatórios ausentes.")

        # ============================================================
        # 3) UF × CEP
        # ============================================================
        validos["compat_uf_cep"] = validos.apply(
            lambda r: self._cep_compat_uf(r["cep"], r["uf"]),
            axis=1
        )

        invalidos_cep = validos[validos["compat_uf_cep"] == False].copy()
        if len(invalidos_cep) > 0:
            invalidos_cep["motivo_invalidade"] = "CEP não compatível com UF"
            logging.warning(f"⚠️ {len(invalidos_cep)} inválidos — UF × CEP inconsistente.")

        validos = validos[validos["compat_uf_cep"] == True].copy()

        # ============================================================
        # 4) Conversões numéricas
        # ============================================================
        validos["clientes_total"] = (
            pd.to_numeric(validos["clientes_total"], errors="coerce").fillna(0).astype(int)
        )

        if "clientes_target" in validos.columns:
            validos["clientes_target"] = (
                pd.to_numeric(validos["clientes_target"], errors="coerce").fillna(0).astype(int)
            )
        else:
            validos["clientes_target"] = 0

        # ============================================================
        # 5) Consolidação final
        # ============================================================
        invalidos = pd.concat([invalidos, invalidos_cep], ignore_index=True)

        for col in ["compat_uf_cep"]:
            if col in validos.columns:
                validos = validos.drop(columns=[col])
            if col in invalidos.columns:
                invalidos = invalidos.drop(columns=[col])

        return validos.reset_index(drop=True), invalidos.reset_index(drop=True)

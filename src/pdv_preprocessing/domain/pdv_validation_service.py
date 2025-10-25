# src/pdv_preprocessing/domain/pdv_validation_service.py

import pandas as pd
import numpy as np
import re
import logging


class PDVValidationService:
    """
    Serviço de validação de PDVs.
    - Limpa e valida CNPJ/CEP.
    - Detecta campos obrigatórios ausentes (exceto 'bairro', que é opcional).
    - Evita duplicados no CSV e no banco para o mesmo tenant.
    """

    def __init__(self, db_reader=None):
        """
        Pode receber o DatabaseReader opcionalmente
        para verificar duplicidades no banco.
        """
        self.db_reader = db_reader

    # ============================================================
    # 🔹 Limpeza de CNPJ e CEP
    # ============================================================
    @staticmethod
    def limpar_cnpj(cnpj: str) -> str:
        """Remove caracteres não numéricos e valida tamanho."""
        if not cnpj or pd.isna(cnpj):
            return None
        cnpj = re.sub(r"[^0-9]", "", str(cnpj))
        return cnpj if len(cnpj) == 14 else None

    @staticmethod
    def limpar_cep(cep: str) -> str:
        """Remove caracteres não numéricos e normaliza para 8 dígitos."""
        if not cep or pd.isna(cep):
            return None
        cep = re.sub(r"[^0-9]", "", str(cep))
        return cep.zfill(8) if len(cep) in (5, 8) else None

        # ============================================================
    # 🔹 Validação principal
    # ============================================================
    def validar_dados(self, df: pd.DataFrame, tenant_id: int = None, input_id: str = None):
        """
        Valida campos obrigatórios e duplicidades (CSV + banco, somente dentro do mesmo input_id).
        Retorna dois DataFrames: válidos e inválidos (com motivo).
        """

        campos_obrigatorios = ["cnpj", "logradouro", "numero", "cidade", "uf", "cep"]

        # 🔹 Normaliza strings vazias
        df[campos_obrigatorios] = df[campos_obrigatorios].replace("", np.nan)

        # ℹ️ Log de auditoria: quantos PDVs estão sem bairro
        if "bairro" in df.columns:
            total_sem_bairro = df["bairro"].eq("").sum()
            logging.info(f"ℹ️ [{tenant_id}] {total_sem_bairro} PDV(s) sem bairro informado.")
        else:
            logging.info(f"ℹ️ [{tenant_id}] Coluna 'bairro' não presente (opcional).")

        # 🔹 Registros com campos obrigatórios faltando
        registros_invalidos = df[df[campos_obrigatorios].isna().any(axis=1)].copy()
        if not registros_invalidos.empty:
            registros_invalidos["motivo_invalidade"] = registros_invalidos.apply(
                lambda row: ", ".join([c for c in campos_obrigatorios if pd.isna(row[c])]),
                axis=1,
            )
            logging.warning(f"⚠️ [{tenant_id}] {len(registros_invalidos)} registro(s) com campos obrigatórios faltando.")

        # 🔹 Mantém apenas válidos
        df_validos = df.dropna(subset=campos_obrigatorios).copy()

        # ============================================================
        # 1️⃣ Duplicados no próprio arquivo
        # ============================================================
        duplicados_csv = df_validos[df_validos.duplicated(subset=["cnpj"], keep=False)].copy()
        if not duplicados_csv.empty:
            duplicados_csv["motivo_invalidade"] = "CNPJ duplicado no arquivo"
            registros_invalidos = pd.concat([registros_invalidos, duplicados_csv])
            df_validos = df_validos[~df_validos["cnpj"].isin(duplicados_csv["cnpj"])]
            logging.warning(f"⚠️ [{tenant_id}] {len(duplicados_csv)} CNPJs duplicados no arquivo CSV.")

        # ============================================================
        # 2️⃣ Duplicados no banco — somente dentro do mesmo input_id
        # ============================================================
        if self.db_reader is not None and tenant_id is not None and input_id is not None:
            try:
                cnpjs_existentes = self.db_reader.buscar_cnpjs_existentes(tenant_id, input_id)
                if cnpjs_existentes:
                    duplicados_banco = df_validos[df_validos["cnpj"].isin(cnpjs_existentes)].copy()
                    if not duplicados_banco.empty:
                        duplicados_banco["motivo_invalidade"] = "CNPJ duplicado neste input_id"
                        registros_invalidos = pd.concat([registros_invalidos, duplicados_banco])
                        df_validos = df_validos[~df_validos["cnpj"].isin(duplicados_banco["cnpj"])]
                        logging.warning(
                            f"⚠️ [{tenant_id}] {len(duplicados_banco)} CNPJs já existentes neste input_id foram ignorados."
                        )
            except Exception as e:
                logging.error(f"❌ [{tenant_id}] Erro ao verificar duplicados no banco: {e}")

        # ============================================================
        # Finalização
        # ============================================================
        registros_invalidos = registros_invalidos.drop_duplicates(subset=["cnpj"]).reset_index(drop=True)
        df_validos = df_validos.reset_index(drop=True)

        logging.info(f"✅ [{tenant_id}] {len(df_validos)} registros válidos / {len(registros_invalidos)} inválidos.")
        return df_validos, registros_invalidos


#sales_router/src/pdv_preprocessing/domain/pdv_validation_service.py

import pandas as pd
import numpy as np
import re
from loguru import logger


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

    CAMPOS_OBRIGATORIOS = ["cnpj", "logradouro", "numero", "cidade", "uf"]

    MOTIVOS_CAMPOS_OBRIGATORIOS = {
        "cnpj": "campo_obrigatorio_cnpj",
        "logradouro": "campo_obrigatorio_logradouro",
        "numero": "campo_obrigatorio_numero",
        "cidade": "campo_obrigatorio_cidade",
        "uf": "campo_obrigatorio_uf",
    }

    MOTIVO_NUMERO_AUSENTE_SEM_CEP = "numero_ausente_sem_cep"
    MOTIVO_CNPJ_DUPLICADO_ARQUIVO = "cnpj_duplicado_arquivo"
    MOTIVO_CNPJ_DUPLICADO_INPUT = "cnpj_duplicado_input_id"

    # ============================================================
    # 🔹 Limpeza de CNPJ e CEP
    # ============================================================
    @staticmethod
    def limpar_cnpj(cnpj: str) -> str | None:
        """Remove caracteres não numéricos e valida tamanho."""
        if not cnpj or pd.isna(cnpj):
            return None
        cnpj = re.sub(r"[^0-9]", "", str(cnpj))
        return cnpj if len(cnpj) == 14 else None

    @staticmethod
    def limpar_cep(cep: str) -> str | None:
        """Remove caracteres não numéricos e normaliza para 8 dígitos."""
        if not cep or pd.isna(cep):
            return None
        cep = re.sub(r"[^0-9]", "", str(cep))
        return cep.zfill(8) if len(cep) in (5, 8) else None

    def _motivo_campos_obrigatorios(self, row: pd.Series) -> str:
        missing_fields = [
            field for field in self.CAMPOS_OBRIGATORIOS if pd.isna(row.get(field))
        ]

        if not missing_fields:
            return "campo_obrigatorio_ausente"

        if len(missing_fields) == 1:
            return self.MOTIVOS_CAMPOS_OBRIGATORIOS.get(
                missing_fields[0],
                f"campo_obrigatorio_{missing_fields[0]}",
            )

        ordered_codes = [
            self.MOTIVOS_CAMPOS_OBRIGATORIOS.get(
                field,
                f"campo_obrigatorio_{field}",
            )
            for field in missing_fields
        ]
        return "|".join(ordered_codes)

    # ============================================================
    # 🔍 Validação principal
    # ============================================================
    def validar_dados(
        self,
        df: pd.DataFrame,
        tenant_id: int | None = None,
        input_id: str | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Valida campos obrigatórios e duplicidades (CSV + banco, somente dentro do mesmo input_id).
        Retorna dois DataFrames: válidos e inválidos (com motivo).
        """

        # ------------------------------------------------------------------
        # CAMPOS REALMENTE OBRIGATÓRIOS
        # ------------------------------------------------------------------
        campos_obrigatorios = self.CAMPOS_OBRIGATORIOS

        # Normaliza strings vazias
        df[campos_obrigatorios] = df[campos_obrigatorios].replace("", np.nan)

        # Log ausência de bairro (opcional)
        if "bairro" in df.columns:
            total_sem_bairro = df["bairro"].eq("").sum()
            logger.info(f"ℹ️ [{tenant_id}] {total_sem_bairro} PDV(s) sem bairro informado.")
        else:
            logger.info(f"ℹ️ [{tenant_id}] Coluna 'bairro' ausente (opcional).")

        # ============================================================
        # 🚫 Campos obrigatórios ausentes
        # ============================================================
        registros_invalidos = df[df[campos_obrigatorios].isna().any(axis=1)].copy()

        if not registros_invalidos.empty:
            registros_invalidos["motivo_invalidade"] = registros_invalidos.apply(
                self._motivo_campos_obrigatorios,
                axis=1,
            )
            logger.warning(
                f"⚠️ [{tenant_id}] {len(registros_invalidos)} registro(s) com campos essenciais faltando."
            )

        # Mantém válidos
        df_validos = df.dropna(subset=campos_obrigatorios).copy()

        # ============================================================
        # 1️⃣ Duplicados no próprio arquivo
        # ============================================================
        duplicados_csv = df_validos[df_validos.duplicated(subset=["cnpj"], keep=False)].copy()
        if not duplicados_csv.empty:
            duplicados_csv["motivo_invalidade"] = self.MOTIVO_CNPJ_DUPLICADO_ARQUIVO
            registros_invalidos = pd.concat(
                [registros_invalidos, duplicados_csv], ignore_index=True
            )
            df_validos = df_validos[~df_validos["cnpj"].isin(duplicados_csv["cnpj"])]
            logger.warning(f"⚠️ [{tenant_id}] {len(duplicados_csv)} CNPJs duplicados no arquivo CSV.")

        # ============================================================
        # 2️⃣ Duplicados no banco — somente dentro do mesmo input_id
        # ============================================================
        if self.db_reader and tenant_id and input_id:
            try:
                cnpjs_existentes = self.db_reader.buscar_cnpjs_existentes(tenant_id, input_id)
                if cnpjs_existentes:
                    duplicados_banco = df_validos[df_validos["cnpj"].isin(cnpjs_existentes)].copy()
                    if not duplicados_banco.empty:
                        duplicados_banco["motivo_invalidade"] = self.MOTIVO_CNPJ_DUPLICADO_INPUT
                        registros_invalidos = pd.concat(
                            [registros_invalidos, duplicados_banco], ignore_index=True
                        )
                        df_validos = df_validos[
                            ~df_validos["cnpj"].isin(duplicados_banco["cnpj"])
                        ]
                        logger.warning(
                            f"⚠️ [{tenant_id}] {len(duplicados_banco)} CNPJs já existentes neste input_id foram ignorados."
                        )
            except Exception as e:
                logger.error(f"❌ [{tenant_id}] Erro ao verificar duplicados no banco: {e}", exc_info=True)

        # ============================================================
        # ✅ Resultado final
        # ============================================================
        registros_invalidos = registros_invalidos.drop_duplicates(subset=["cnpj"]).reset_index(
            drop=True
        )
        df_validos = df_validos.reset_index(drop=True)

        logger.info(f"✅ [{tenant_id}] {len(df_validos)} válidos / {len(registros_invalidos)} inválidos após validação.")
        return df_validos, registros_invalidos

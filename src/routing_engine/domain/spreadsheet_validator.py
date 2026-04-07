#sales_router/src/routing_engine/domain/spreadsheet_validator.py

from __future__ import annotations

from dataclasses import dataclass
from typing import List

import pandas as pd

from routing_engine.domain.utils_geo import normalize_coord, is_valid_lat_lon
from routing_engine.application.consultor_service import ConsultorService


REQUIRED_COLUMNS_BASE = [
    "cnpj",
    "logradouro",
    "numero",
    "bairro",
    "cidade",
    "uf",
    "cep",
    "lat",
    "lon",
    "consultor",  # 🔥 obrigatório
]

OPTIONAL_COLUMNS = [
    "nome_fantasia",
    "setor",  # opcional
]


@dataclass
class ValidationResult:
    dataframe: pd.DataFrame
    fonte_grupo: str
    warnings: List[str]


class SpreadsheetValidator:

    def __init__(self, tenant_id: int = 1) -> None:
        self.warnings: List[str] = []
        self.tenant_id = tenant_id

    def validate(self, df: pd.DataFrame) -> ValidationResult:

        df = df.copy()
        df.columns = [self._normalize_col(c) for c in df.columns]

        self._validate_required_columns(df)

        df = self._normalize_strings(df)

        # =========================================================
        # 🔥 CONSULTOR OBRIGATÓRIO E NÃO VAZIO
        # =========================================================
        invalid_consultor = df["consultor"].isna() | (df["consultor"].astype(str).str.strip() == "")

        if invalid_consultor.any():
            idxs = list(df[invalid_consultor].index[:10] + 2)
            raise ValueError(f"Coluna 'consultor' possui valores vazios. Linhas exemplo: {idxs}")

        # =========================================================
        # 🔥 NORMALIZA CONSULTOR
        # =========================================================
        df["consultor"] = df["consultor"].astype(str).str.strip().str.upper()

        # =========================================================
        # 🔥 VALIDA CONSULTOR NA BASE
        # =========================================================
        consultor_service = ConsultorService(self.tenant_id)
        consultores_validos = set(consultor_service.get_all_consultores())

        invalidos = ~df["consultor"].isin(consultores_validos)

        if invalidos.any():
            exemplos = df.loc[invalidos, "consultor"].dropna().unique()[:5]
            raise ValueError(f"Consultores não cadastrados: {list(exemplos)}")

        # =========================================================
        # 🔥 DEFINE GRUPO (SEMPRE CONSULTOR)
        # =========================================================
        df["grupo_utilizado"] = df["consultor"]
        df["fonte_grupo"] = "consultor"

        # =========================================================
        # 🔥 COORDENADAS
        # =========================================================
        df = self._normalize_coordinates(df)

        # =========================================================
        # 🔥 VALORES OBRIGATÓRIOS
        # =========================================================
        df = self._validate_required_values(df)

        # =========================================================
        # 🔥 DEDUPLICAÇÃO
        # =========================================================
        df = self._deduplicate(df)

        # =========================================================
        # 🔥 COLUNAS FINAIS
        # =========================================================
        df["nome_fantasia"] = df["nome_fantasia"] if "nome_fantasia" in df.columns else None

        ordered_cols = [
            "cnpj",
            "nome_fantasia",
            "logradouro",
            "numero",
            "bairro",
            "cidade",
            "uf",
            "cep",
            "grupo_utilizado",
            "fonte_grupo",
            "lat",
            "lon",
        ]

        df = df[ordered_cols]

        return ValidationResult(
            dataframe=df.reset_index(drop=True),
            fonte_grupo="consultor",
            warnings=self.warnings,
        )

    # =========================================================
    # INTERNOS
    # =========================================================

    def _validate_required_columns(self, df: pd.DataFrame) -> None:
        missing = [c for c in REQUIRED_COLUMNS_BASE if c not in df.columns]
        if missing:
            raise ValueError(f"Colunas obrigatórias ausentes: {', '.join(missing)}")

    def _normalize_col(self, col: str) -> str:
        return (
            str(col)
            .strip()
            .lower()
            .replace("ç", "c")
            .replace("ã", "a")
            .replace("á", "a")
            .replace("à", "a")
            .replace("â", "a")
            .replace("é", "e")
            .replace("ê", "e")
            .replace("í", "i")
            .replace("ó", "o")
            .replace("ô", "o")
            .replace("õ", "o")
            .replace("ú", "u")
        )

    def _normalize_strings(self, df: pd.DataFrame) -> pd.DataFrame:

        text_cols = [
            "cnpj",
            "nome_fantasia",
            "logradouro",
            "numero",
            "bairro",
            "cidade",
            "uf",
            "cep",
            "setor",
            "consultor",
        ]

        for col in text_cols:
            if col in df.columns:
                df[col] = df[col].apply(self._clean_text)

        return df

    def _clean_text(self, value):
        if pd.isna(value):
            return None
        value = str(value).strip()
        if value.lower() in {"nan", "none", ""}:
            return None
        return value

    def _normalize_coordinates(self, df: pd.DataFrame) -> pd.DataFrame:

        lat_norm = []
        lon_norm = []

        for idx, row in df.iterrows():
            lat = row.get("lat")
            lon = row.get("lon")

            try:
                lat_f, lon_f = normalize_coord(float(lat), float(lon))
            except Exception:
                raise ValueError(f"Linha {idx + 2}: coordenadas inválidas ({lat}, {lon})")

            if not is_valid_lat_lon(lat_f, lon_f):
                raise ValueError(
                    f"Linha {idx + 2}: coordenadas fora do Brasil ({lat_f}, {lon_f})"
                )

            lat_norm.append(lat_f)
            lon_norm.append(lon_f)

        df["lat"] = lat_norm
        df["lon"] = lon_norm

        return df

    def _validate_required_values(self, df: pd.DataFrame) -> pd.DataFrame:

        required_value_cols = [
            "cnpj",
            "logradouro",
            "cidade",
            "uf",
            "grupo_utilizado",
            "lat",
            "lon",
        ]

        for col in required_value_cols:
            invalid = df[col].isna() | (df[col].astype(str).str.strip() == "")
            if invalid.any():
                idxs = list(df[invalid].index[:10] + 2)
                raise ValueError(
                    f"Coluna '{col}' possui valores vazios. Linhas exemplo: {idxs}"
                )

        return df

    def _deduplicate(self, df: pd.DataFrame) -> pd.DataFrame:

        original = len(df)

        df = df.drop_duplicates(
            subset=["cnpj", "grupo_utilizado"],
            keep="first",
        ).copy()

        removed = original - len(df)

        if removed > 0:
            self.warnings.append(
                f"{removed} linha(s) duplicada(s) removida(s) por cnpj + grupo_utilizado."
            )

        return df
#sales_router/src/geocoding_engine/application/geocode_spreadsheet_use_case.py

import pandas as pd
from loguru import logger

from geocoding_engine.application.geocode_addresses_use_case import GeocodeAddressesUseCase
from geocoding_engine.utils.excel_exporter import ExcelExporter
from geocoding_engine.domain.address_normalizer import normalize_for_geocoding, normalize_for_cache
from geocoding_engine.domain.utils_geo import cep_invalido


class GeocodeSpreadsheetUseCase:

    REQUIRED_ADDRESS = [
        "logradouro",
        "numero",
        "bairro",
        "cidade",
        "uf",
        "cep"
    ]

    REQUIRED_IDENT = [
        "cnpj",
        "razao_social",
        "nome_fantasia"
    ]

    REQUIRED_RESP = [
        "consultor",
        "setor"
    ]

    # ---------------------------------------------------------
    def _validar(self, df):

        missing = [c for c in self.REQUIRED_ADDRESS if c not in df.columns]

        if missing:
            raise Exception(f"Colunas obrigatórias ausentes: {missing}")

        for col in self.REQUIRED_RESP + self.REQUIRED_IDENT:
            if col not in df.columns:
                df[col] = None

        return df

    # ---------------------------------------------------------
    def _safe_str(self, v):
        if pd.isna(v):
            return ""
        return str(v).strip()

    # ---------------------------------------------------------
    def _montar_endereco_completo(self, row):
        log = self._safe_str(row["logradouro"])
        num = self._safe_str(row["numero"])
        bairro = self._safe_str(row["bairro"])
        cidade = self._safe_str(row["cidade"])
        uf = self._safe_str(row["uf"])

        if not log or not cidade or not uf:
            return ""

        if num:
            raw = f"{log} {num}, {bairro}, {cidade} - {uf}"
        else:
            raw = f"{log}, {bairro}, {cidade} - {uf}"

        return raw

    # ---------------------------------------------------------
    
    # ---------------------------------------------------------
    def execute(self, df: pd.DataFrame, progress_callback=None):

        logger.info(f"[SPREADSHEET] linhas recebidas={len(df)}")

        df = self._validar(df).copy()
        df.reset_index(drop=True, inplace=True)
        df["__id"] = df.index

        # =========================================================
        # 🔥 MONTAR ESTRUTURA
        # =========================================================
        items = []

        for _, row in df.iterrows():

            items.append({
                "id": row["__id"],
                "query_full": self._montar_endereco_completo(row),
                "cidade": row["cidade"],
                "uf": row["uf"],
                "cep": row["cep"]
            })

        # =========================================================
        # 🔥 MAPA DE ENDEREÇO PARA VISUALIZAÇÃO  👈 AQUI
        # =========================================================
        map_endereco = {
            item["id"]: item["query_full"]
            for item in items
        }

        df["endereco"] = df["__id"].map(map_endereco)

        # =========================================================
        # 🔥 DEDUP (COM CEP)
        # =========================================================
        dedup_map = {}
        reverse_map = {}

        for item in items:

            key = normalize_for_cache(item["query_full"], cep=item["cep"])

            if key not in dedup_map:
                dedup_map[key] = item

            reverse_map[item["id"]] = key

        unique_items = list(dedup_map.values())

        logger.info(f"[DEDUP] original={len(df)} unico={len(unique_items)}")

        if progress_callback:
            progress_callback(10, "Preparando geocodificação")

        uc = GeocodeAddressesUseCase()
        result_map = {}

        # =========================================================
        # 🔥 STAGE 1 - ENDEREÇO COMPLETO
        # =========================================================
        payload = [
            {
                "id": item["id"],
                "address": item["query_full"],
                "cidade": item["cidade"],
                "uf": item["uf"],
                "cep": item["cep"]
            }
            for item in unique_items if item["query_full"]
        ]

        res = uc.execute(payload)

        for r in res["results"]:
            result_map[r["id"]] = r

        
        # =========================================================
        # 🔥 EXPANSÃO
        # =========================================================
        df["lat"] = df["__id"].map(lambda i: result_map.get(i, {}).get("lat"))
        df["lon"] = df["__id"].map(lambda i: result_map.get(i, {}).get("lon"))
        df["geocode_source"] = df["__id"].map(lambda i: result_map.get(i, {}).get("source"))

        # =========================================================
        # 🔥 VALIDAÇÃO FINAL
        # =========================================================
        df_validos = df[df["lat"].notnull()].copy()

        if not df_validos.empty:
            df_validos_final = df_validos.copy()
            df_invalidos_municipio = pd.DataFrame()
        else:
            df_validos_final = df_validos
            df_invalidos_municipio = pd.DataFrame()

        df_invalidos_geocode = df[df["lat"].isnull()].copy()

        df_invalidos = pd.concat(
            [df_invalidos_geocode, df_invalidos_municipio],
            ignore_index=True
        )

        stats = {
            "total": len(df),
            "unicos": len(unique_items),
            "sucesso": len(df_validos_final),
            "falhas": len(df_invalidos)
        }

        logger.info(f"[STATS] {stats}")

        if progress_callback:
            progress_callback(90, "Salvando resultado")

        excel_buffer = ExcelExporter.gerar_excel(
            df_validos_final,
            df_invalidos
        )

        return excel_buffer, stats
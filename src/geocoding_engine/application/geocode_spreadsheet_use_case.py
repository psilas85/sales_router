#sales_router/src/geocoding_engine/application/geocode_spreadsheet_use_case.py

import pandas as pd

from geocoding_engine.application.geocode_addresses_use_case import GeocodeAddressesUseCase
from geocoding_engine.domain.address_normalizer import normalize_base


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

    def _validar(self, df):

        missing = []

        for col in self.REQUIRED_ADDRESS:
            if col not in df.columns:
                missing.append(col)

        if missing:
            raise Exception(f"Colunas obrigatórias ausentes: {missing}")

        for col in self.REQUIRED_RESP:
            if col not in df.columns:
                df[col] = None

        for col in self.REQUIRED_IDENT:
            if col not in df.columns:
                df[col] = None

        return df


    def _montar_endereco(self, row):

        return f"{row.logradouro} {row.numero}, {row.bairro}, {row.cidade} - {row.uf}"


    def execute(self, df: pd.DataFrame):

        df = self._validar(df)

        addresses = []

        for i, row in df.iterrows():

            endereco = self._montar_endereco(row)

            addresses.append({
                "id": i,
                "address": endereco
            })

        uc = GeocodeAddressesUseCase()

        res = uc.execute(addresses)

        lat = []
        lon = []
        source = []

        for r in res["results"]:
            lat.append(r["lat"])
            lon.append(r["lon"])
            source.append(r["source"])

        df["lat"] = lat
        df["lon"] = lon
        df["geocode_source"] = source

        stats = {
            "total": len(df),
            "sucesso": df["lat"].notnull().sum(),
            "falhas": df["lat"].isnull().sum()
        }

        return df, stats
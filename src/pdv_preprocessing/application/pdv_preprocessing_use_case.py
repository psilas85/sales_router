# src/pdv_preprocessing/application/pdv_preprocessing_use_case.py

import pandas as pd
import logging
import unicodedata
from pdv_preprocessing.entities.pdv_entity import PDV
from pdv_preprocessing.domain.pdv_validation_service import PDVValidationService
from pdv_preprocessing.domain.geolocation_service import GeolocationService


class PDVPreprocessingUseCase:
    def __init__(self, reader, writer, tenant_id):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.validator = PDVValidationService()
        self.geo_service = GeolocationService(reader, writer)

    def normalizar_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Padroniza nomes de colunas para minúsculas e sem acentos."""
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .map(lambda x: unicodedata.normalize("NFKD", x).encode("ascii", errors="ignore").decode("utf-8"))
        )
        return df

    def execute(self, input_path: str, sep=";"):
        logging.info(f"📄 Lendo arquivo de entrada: {input_path}")

        # Carrega CSV
        df = pd.read_csv(input_path, sep=sep, dtype=str).fillna("")
        df = self.normalizar_colunas(df)

        # 🔍 Verifica colunas obrigatórias
        colunas_esperadas = ["cnpj", "logradouro", "numero", "bairro", "cidade", "uf", "cep"]
        faltantes = [col for col in colunas_esperadas if col not in df.columns]
        if faltantes:
            raise ValueError(f"❌ Colunas obrigatórias ausentes no CSV: {', '.join(faltantes)}")

        # 🧹 Limpeza e formatação
        df["cnpj"] = df["cnpj"].apply(self.validator.limpar_cnpj)
        df["cep"] = df["cep"].apply(self.validator.limpar_cep)

        df["pdv_endereco_completo"] = (
            df["logradouro"].astype(str).str.strip() + ", " +
            df["numero"].astype(str).str.strip() + ", " +
            df["bairro"].astype(str).str.strip() + ", " +
            df["cidade"].astype(str).str.strip() + " - " +
            df["uf"].astype(str).str.strip() + ", " +
            df["cep"].astype(str).str.strip()
        )

        # 🧾 Validação de registros
        df_validos, df_invalidos = self.validator.validar_dados(df)
        logging.info(f"✅ {len(df_validos)} registros válidos / {len(df_invalidos)} inválidos")

        if df_validos.empty:
            logging.warning("⚠️ Nenhum PDV válido para geolocalização.")
            return df_validos, df_invalidos

        # 📍 Busca coordenadas (cache → Nominatim → Google)
        coords = []
        for _, row in df_validos.iterrows():
            lat, lon, origem = self.geo_service.buscar_coordenadas(row["pdv_endereco_completo"], row["uf"])
            coords.append((lat, lon, origem))

        df_validos["pdv_lat"], df_validos["pdv_lon"], df_validos["status_geolocalizacao"] = zip(*coords)

        # 🗄️ Criação das entidades PDV
        campos_validos = PDV.__init__.__code__.co_varnames[1:]  # ignora self
        colunas_validas = [c for c in df_validos.columns if c in campos_validos]

        # adiciona tenant_id se não estiver no DataFrame
        df_validos["tenant_id"] = self.tenant_id

        # reduz o DataFrame somente às colunas aceitas pela entidade
        df_para_inserir = df_validos[colunas_validas + ["tenant_id"]]

        pdvs = [PDV(**row) for row in df_para_inserir.to_dict(orient="records")]


        # 💾 Inserção / atualização no banco
        self.writer.inserir_pdvs(pdvs)

        logging.info("💾 PDVs inseridos/atualizados com sucesso no banco.")
        return df_validos, df_invalidos

# ============================================================
# 📦 src/pdv_preprocessing/application/mkp_preprocessing_use_case.py
# ============================================================

import os
import pandas as pd
import logging
import unicodedata
from pdv_preprocessing.entities.mkp_entity import MKP
from pdv_preprocessing.domain.mkp_validation_service import MKPValidationService
from pdv_preprocessing.domain.geolocation_service import GeolocationService
from pdv_preprocessing.config.uf_bounds import UF_BOUNDS


class MKPPreprocessingUseCase:
    """
    Caso de uso principal do pré-processamento de dados de marketplace (agregados por CEP).
    Inclui:
      - Normalização e limpeza
      - Validação cadastral
      - Georreferenciamento otimizado por CEP (com cache global)
      - Validação UF × coordenadas
      - Inserção no banco com tenant_id, input_id e descrição
    """

    def __init__(self, reader, writer, tenant_id, input_id=None, descricao=None):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.input_id = input_id
        self.descricao = descricao
        self.validator = MKPValidationService()
        # aumenta número de workers para paralelismo real
        self.geo_service = GeolocationService(reader, writer, max_workers=30)

    # ------------------------------------------------------------
    # Normalização de colunas
    # ------------------------------------------------------------
    def normalizar_colunas(self, df):
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .map(lambda x: unicodedata.normalize("NFKD", x)
                 .encode("ascii", errors="ignore")
                 .decode("utf-8"))
        )
        return df

    # ------------------------------------------------------------
    # Limpeza de valores
    # ------------------------------------------------------------
    def limpar_valores(self, df):
        for col in ["cidade", "uf", "bairro"]:
            if col in df.columns:
                df[col] = df[col].astype(str).str.strip().replace({"nan": "", "None": ""})

        if "uf" in df.columns:
            df["uf"] = df["uf"].str.upper().str.strip()

        if "cidade" in df.columns:
            df["cidade"] = df["cidade"].apply(
                lambda x: unicodedata.normalize("NFKD", str(x))
                .encode("ascii", errors="ignore")
                .decode("utf-8")
                .upper()
                .strip()
            )

        # Conversão de campos numéricos
        for campo in ["clientes_total", "clientes_target"]:
            if campo in df.columns:
                df[campo] = pd.to_numeric(df[campo], errors="coerce")

        return df

    # ------------------------------------------------------------
    # Execução principal
    # ------------------------------------------------------------
    def execute(self, input_path: str, sep=";", input_id=None, descricao=None):
        logging.info(f"📄 Lendo arquivo: {input_path}")
        df = pd.read_csv(input_path, sep=sep, dtype=str).fillna("")
        df = self.normalizar_colunas(df)
        df = self.limpar_valores(df)

        # ============================================================
        # 🧩 Validação cadastral
        # ============================================================
        validos, invalidos = self.validator.validar_dados(df, tenant_id=self.tenant_id)
        if validos.empty:
            logging.warning(f"⚠️ [{self.tenant_id}] Nenhum registro válido para georreferenciamento.")
            return validos, invalidos, 0

        validos["tenant_id"] = self.tenant_id
        validos["input_id"] = self.input_id
        validos["descricao"] = self.descricao

        # ============================================================
        # 🌍 Georreferenciamento otimizado (cache MKP global)
        # ============================================================
        validos["lat"] = None
        validos["lon"] = None
        validos["status_geolocalizacao"] = None

        # 1️⃣ Identifica CEPs únicos
        ceps_unicos = (
            validos["cep"]
            .dropna()
            .astype(str)
            .str.zfill(8)
            .unique()
            .tolist()
        )
        logging.info(f"🧮 {len(ceps_unicos)} CEPs únicos encontrados para georreferenciamento.")

        # 2️⃣ Busca CEPs já existentes no cache MKP (banco)
        cache_db = self.reader.buscar_localizacoes_mkp_por_ceps(ceps_unicos)
        cache_encontrado = {
            str(row["cep"]).zfill(8): (row["lat"], row["lon"])
            for row in cache_db
            if row.get("lat") is not None and row.get("lon") is not None
        }

        logging.info(f"🗄️ {len(cache_encontrado)} CEPs encontrados no cache global (mkp_enderecos_cache).")

        # 3️⃣ Atribui coordenadas de cache aos registros correspondentes
        for i, row in validos.iterrows():
            cep = str(row["cep"]).zfill(8)
            if cep in cache_encontrado:
                lat, lon = cache_encontrado[cep]
                validos.at[i, "lat"] = lat
                validos.at[i, "lon"] = lon
                validos.at[i, "status_geolocalizacao"] = "cache_db"

        # 4️⃣ Identifica os CEPs faltantes (sem cache)
        faltantes = validos[validos["lat"].isna()]
        logging.info(f"🌐 {len(faltantes)} CEPs sem cache — iniciando geocodificação externa (Nominatim/Google).")

        # ⚡️ Novo modo: geocodificação em paralelo (batch)
        faltantes_ceps = faltantes["cep"].dropna().astype(str).str.zfill(8).unique().tolist()
        if len(faltantes_ceps) > 0:
            logging.info(f"⚡ Geocodificando {len(faltantes_ceps)} CEPs em paralelo (max {self.geo_service.max_workers} threads)...")
            resultados_geo = self.geo_service.geocodificar_em_lote(faltantes_ceps, tipo="MKP")

            for i, row in validos.iterrows():
                cep = str(row.get("cep", "")).zfill(8)
                if cep in resultados_geo:
                    lat, lon = resultados_geo[cep]
                    validos.at[i, "lat"] = lat
                    validos.at[i, "lon"] = lon
                    validos.at[i, "status_geolocalizacao"] = "nominatim_public"
        else:
            logging.info("✅ Todos os CEPs já estavam em cache, nenhum faltante a geocodificar.")

        logging.info(f"📍 Georreferenciamento concluído: {len(validos)} registros processados.")

        # ============================================================
        # 🧭 Validação UF × Coordenadas
        # ============================================================
        def validar_limites_uf(row):
            if pd.isna(row["lat"]) or pd.isna(row["lon"]):
                return "falha_geolocalizacao"
            bounds = UF_BOUNDS.get(row["uf"])
            if not bounds:
                return "uf_invalida"
            if not (bounds["lat_min"] <= row["lat"] <= bounds["lat_max"]
                    and bounds["lon_min"] <= row["lon"] <= bounds["lon_max"]):
                return "coordenadas_fora_limites"
            return "ok"

        validos["motivo_invalidade_geo"] = validos.apply(validar_limites_uf, axis=1)
        invalidos_geo = validos[validos["motivo_invalidade_geo"] != "ok"]
        validos = validos[validos["motivo_invalidade_geo"] == "ok"]

        if not invalidos_geo.empty:
            invalidos = pd.concat([invalidos, invalidos_geo], ignore_index=True)
            logging.warning(f"⚠️ {len(invalidos_geo)} registros com coordenadas fora dos limites da UF.")

        # ============================================================
        # 💾 Inserção no banco de dados
        # ============================================================
        mkps = [MKP(**row) for row in validos.to_dict(orient="records")]
        inseridos = self.writer.inserir_mkp(mkps)

        logging.info(f"✅ {len(validos)} válidos / {len(invalidos)} inválidos.")
        logging.info(f"💾 {inseridos} registros gravados na tabela marketplace_cep.")
        logging.info("📊 Processo MKP finalizado com sucesso.")

        return validos, invalidos, inseridos

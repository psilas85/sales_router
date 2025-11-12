import os
import pandas as pd
import unicodedata
import re
import time
import uuid
from loguru import logger
from pdv_preprocessing.entities.pdv_entity import PDV
from pdv_preprocessing.domain.pdv_validation_service import PDVValidationService
from pdv_preprocessing.domain.geolocation_service import GeolocationService
from pdv_preprocessing.config.uf_bounds import UF_BOUNDS


class PDVPreprocessingUseCase:
    """
    Caso de uso principal do pré-processamento de PDVs.
    Inclui:
      - Normalização e limpeza de dados
      - Validação cadastral
      - Geocodificação com cache
      - Validação UF × coordenadas
      - Inserção no banco vinculada a um input_id (sem sobrescrita)
    """

    def __init__(self, reader, writer, tenant_id, input_id=None, descricao=None):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.input_id = input_id or str(uuid.uuid4())
        self.descricao = descricao or "PDV Importado"
        self.validator = PDVValidationService(db_reader=reader)
        self.geo_service = GeolocationService(reader, writer)

    # ============================================================
    # 🔹 Normalização de colunas
    # ============================================================
    def normalizar_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .map(lambda x: unicodedata.normalize("NFKD", x)
                 .encode("ascii", errors="ignore")
                 .decode("utf-8"))
        )
        return df

    # ============================================================
    # 🔹 Limpeza de valores e formatos
    # ============================================================
    def limpar_valores(self, df: pd.DataFrame) -> pd.DataFrame:
        def normalizar_cnpj(valor):
            if pd.isna(valor) or str(valor).strip() == "":
                return None
            v = str(valor).strip()
            if re.match(r"^\d+,\d+E\+\d+$", v):
                v = v.replace(",", ".")
            try:
                if "E+" in v or "e+" in v:
                    v = f"{float(v):.0f}"
            except Exception:
                pass
            return re.sub(r"[^0-9]", "", v)

        # Normaliza CNPJ e CEP
        df["cnpj"] = df["cnpj"].apply(normalizar_cnpj)
        if "cep" in df.columns:
            df["cep"] = df["cep"].astype(str).str.replace(r"[^0-9]", "", regex=True)

        # Normaliza campos de texto
        for c in ["logradouro", "bairro", "cidade", "uf", "numero"]:
            if c in df.columns:
                df[c] = df[c].astype(str).str.strip().replace({"nan": "", "None": ""})

        # Normaliza UF
        estados_validos = set(UF_BOUNDS.keys())
        if "uf" in df.columns:
            df["uf"] = df["uf"].str.upper().str.strip()
            uf_invalidas = df.loc[~df["uf"].isin(estados_validos) & df["uf"].ne(""), "uf"].unique()
            if len(uf_invalidas) > 0:
                logger.warning(f"⚠️ UFs inválidas detectadas: {', '.join(uf_invalidas)}")

        # Normaliza cidade
        if "cidade" in df.columns:
            df["cidade"] = df["cidade"].apply(
                lambda x: unicodedata.normalize("NFKD", str(x))
                .encode("ascii", errors="ignore")
                .decode("utf-8")
                .upper()
                .strip()
            )

        # Converte pdv_vendas se existir
        if "pdv_vendas" in df.columns:
            import math

            def normalizar_vendas(valor):
                if pd.isna(valor):
                    return None
                v = str(valor).strip().replace("R$", "").replace("r$", "").strip()
                v = v.replace(".", "").replace(",", ".")
                v = re.sub(r"[^0-9.]", "", v)
                if v == "":
                    return None
                try:
                    num = float(v)
                    if math.isnan(num) or math.isinf(num):
                        return None
                    return num
                except ValueError:
                    return None

            df["pdv_vendas"] = df["pdv_vendas"].apply(normalizar_vendas)
            vendas_validas = df["pdv_vendas"].notna().sum()
            logger.info(f"ℹ️ {vendas_validas} registros com valor de vendas numérico válido.")

        return df

    # ============================================================
    # 🔹 Filtra apenas colunas relevantes
    # ============================================================
    def filtrar_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        colunas_base = ["cnpj", "logradouro", "numero", "bairro", "cidade", "uf", "cep"]
        colunas_opcionais = ["pdv_vendas"]
        colunas_presentes = [c for c in (colunas_base + colunas_opcionais) if c in df.columns]
        return df[colunas_presentes].copy()

    # ============================================================
    # 🔹 Execução principal
    # ============================================================
    def execute(self, input_path: str, sep=";") -> tuple:
        logger.info(f"📄 Lendo arquivo de entrada: {input_path}")
        df = pd.read_csv(input_path, sep=sep, dtype=str).fillna("")
        df = self.normalizar_colunas(df)
        df = self.limpar_valores(df)
        df = self.filtrar_colunas(df)

        colunas_esperadas = ["cnpj", "logradouro", "numero", "cidade", "uf", "cep"]
        faltantes = [col for col in colunas_esperadas if col not in df.columns]
        if faltantes:
            raise ValueError(f"❌ Colunas obrigatórias ausentes: {', '.join(faltantes)}")

        # Monta endereço completo
        df["pdv_endereco_completo"] = df.apply(
            lambda r: ", ".join(
                filter(None, [
                    f"{r['logradouro'].strip()}, {r['numero'].strip()}",
                    str(r.get('bairro', '')).strip(),
                    f"{r['cidade'].strip()} - {r['uf'].strip()}",
                    r["cep"].strip()
                ])
            ),
            axis=1,
        )

        # Validação inicial
        df_validos, df_invalidos = self.validator.validar_dados(df, tenant_id=self.tenant_id)
        if df_validos.empty:
            logger.warning(f"⚠️ [{self.tenant_id}] Nenhum PDV válido para geolocalização.")
            return df_validos, df_invalidos, 0

        # Busca cache
        enderecos_norm = df_validos["pdv_endereco_completo"].str.strip().str.lower().tolist()
        cache_db = self.reader.buscar_enderecos_cache(enderecos_norm)

        df_validos["pdv_lat"] = None
        df_validos["pdv_lon"] = None
        df_validos["status_geolocalizacao"] = None
        enderecos_novos = []

        for i, row in df_validos.iterrows():
            endereco_norm = row["pdv_endereco_completo"].strip().lower()
            if endereco_norm in cache_db:
                lat, lon = cache_db[endereco_norm]
                df_validos.at[i, "pdv_lat"] = lat
                df_validos.at[i, "pdv_lon"] = lon
                df_validos.at[i, "status_geolocalizacao"] = "cache_db"
            else:
                enderecos_novos.append(i)

        logger.info(f"⚡ {len(cache_db)} endereços encontrados no cache.")
        logger.info(f"🌍 {len(enderecos_novos)} endereços novos para geocodificação.")

        # Geocodificação paralela
        if enderecos_novos:
            enderecos_para_geo = [
                df_validos.iloc[i]["pdv_endereco_completo"] for i in enderecos_novos
            ]

            resultados_geo = self.geo_service.geocodificar_em_lote(enderecos_para_geo, tipo="PDV")

            for i in enderecos_novos:
                endereco = df_validos.iloc[i]["pdv_endereco_completo"]
                if endereco in resultados_geo:
                    lat, lon, origem = resultados_geo[endereco]
                    if lat is not None and lon is not None:
                        df_validos.at[i, "pdv_lat"] = lat
                        df_validos.at[i, "pdv_lon"] = lon
                        df_validos.at[i, "status_geolocalizacao"] = origem
                        try:
                            self.writer.inserir_localizacao(endereco, lat, lon)
                            time.sleep(0.05)
                        except Exception as e:
                            logger.warning(f"⚠️ Falha ao salvar no cache: {e}")
                    else:
                        df_validos.at[i, "status_geolocalizacao"] = "falha"
                else:
                    df_validos.at[i, "status_geolocalizacao"] = "falha"

            logger.info(f"✅ Geocodificação concluída: {len(resultados_geo)} endereços resolvidos.")
        else:
            logger.info("⚡ Nenhum endereço novo para geocodificação.")

        # Validação geográfica (UF × coordenadas)
        def validar_limites_uf(row):
            if pd.isna(row["pdv_lat"]) or pd.isna(row["pdv_lon"]):
                return "falha_geolocalizacao"
            bounds = UF_BOUNDS.get(row["uf"])
            if not bounds:
                return "uf_invalida"
            if not (bounds["lat_min"] <= row["pdv_lat"] <= bounds["lat_max"]
                    and bounds["lon_min"] <= row["pdv_lon"] <= bounds["lon_max"]):
                return "coordenadas_fora_limites"
            return "ok"

        df_validos["motivo_invalidade"] = df_validos.apply(validar_limites_uf, axis=1)
        df_invalidos_geo = df_validos[df_validos["motivo_invalidade"] != "ok"]
        df_validos = df_validos[df_validos["motivo_invalidade"] == "ok"]
        df_invalidos_total = pd.concat([df_invalidos, df_invalidos_geo], ignore_index=True)

        # Inserção no banco
        df_validos["tenant_id"] = self.tenant_id
        df_validos["input_id"] = self.input_id
        df_validos["descricao"] = self.descricao

        campos_validos = PDV.__init__.__code__.co_varnames[1:]
        df_para_inserir = df_validos[[c for c in df_validos.columns if c in campos_validos]]

        pdvs = [PDV(**row) for row in df_para_inserir.to_dict(orient="records")]

        try:
            inseridos = self.writer.inserir_pdvs(pdvs)
        except Exception as e:
            if hasattr(self.writer, "conn"):
                self.writer.conn.rollback()
            logger.error(f"❌ Falha ao inserir PDVs no banco: {e}", exc_info=True)
            inseridos = 0

        total_falhas = df_validos["status_geolocalizacao"].eq("falha").sum()
        logger.info(f"✅ [{self.tenant_id}] {len(df_validos)} válidos / {len(df_invalidos_total)} inválidos.")
        logger.info(f"💾 [{self.tenant_id}] {inseridos} PDVs inseridos (input_id={self.input_id}).")
        logger.info(f"⚠️ {total_falhas} PDVs não geocodificados com sucesso.")

        self.geo_service.exibir_resumo_logs()

        return df_validos, df_invalidos_total, inseridos

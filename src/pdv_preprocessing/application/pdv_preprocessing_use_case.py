#sales_router/src/pdv_preprocessing/application/pdv_preprocessing_use_case.py

# ============================================================
# 📦 src/pdv_preprocessing/application/pdv_preprocessing_use_case.py
# ============================================================

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
from pdv_preprocessing.domain.utils_texto import fix_encoding


class PDVPreprocessingUseCase:
    def __init__(self, reader, writer, tenant_id, input_id=None, descricao=None, usar_google=False):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.input_id = input_id or str(uuid.uuid4())
        self.descricao = descricao or "PDV Importado"
        self.usar_google = usar_google

        self.validator = PDVValidationService(db_reader=reader)
        self.geo_service = GeolocationService(reader, writer, usar_google=self.usar_google)
      

    # ------------------------------------------------------------
    # Normalização de colunas
    # ------------------------------------------------------------
    def normalizar_colunas(self, df):
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.normalize("NFC")
        )
        return df

    # ------------------------------------------------------------
    # Limpeza de valores
    # ------------------------------------------------------------
    def limpar_valores(self, df: pd.DataFrame) -> pd.DataFrame:

        # --------------------------------------------------------
        # 🔹 Função para gerar CNPJ válido
        # --------------------------------------------------------
        def gerar_cnpj_valido(seed):
            import random

            random.seed(seed)

            raiz = f"{random.randint(10, 99)}{random.randint(100, 999)}{random.randint(100, 999)}"
            filial = "0001"
            base = raiz + filial

            def calcular_dv(cnpj):
                pesos_1 = [5,4,3,2,9,8,7,6,5,4,3,2]
                pesos_2 = [6] + pesos_1

                soma1 = sum(int(a)*b for a, b in zip(cnpj, pesos_1))
                d1 = 11 - (soma1 % 11)
                d1 = d1 if d1 < 10 else 0

                soma2 = sum(int(a)*b for a, b in zip(cnpj + str(d1), pesos_2))
                d2 = 11 - (soma2 % 11)
                d2 = d2 if d2 < 10 else 0

                return f"{d1}{d2}"

            dv = calcular_dv(base)
            return base + dv

        # --------------------------------------------------------
        # 🔹 Normalização de CNPJ
        # --------------------------------------------------------
        def normalizar_cnpj(valor):
            if pd.isna(valor) or str(valor).strip() == "":
                return None

            v = fix_encoding(str(valor).strip())

            if re.match(r"^\d+,\d+E\+\d+$", v):
                v = v.replace(",", ".")

            try:
                if "E+" in v or "e+" in v:
                    v = f"{float(v):.0f}"
            except Exception:
                pass

            return re.sub(r"[^0-9]", "", v)

        # Se a coluna não existir no CSV → cria
        if "cnpj" not in df.columns:
            df["cnpj"] = ""

        df["cnpj"] = df["cnpj"].apply(normalizar_cnpj)

        # --------------------------------------------------------
        # 🔹 Preenchimento com CNPJ sintético quando vazio
        # --------------------------------------------------------
        for idx, val in df["cnpj"].items():
            if val in (None, "", "nan"):
                df.at[idx, "cnpj"] = gerar_cnpj_valido(idx)

        # --------------------------------------------------------
        # CEP
        # --------------------------------------------------------
        if "cep" in df.columns:
            df["cep"] = (
                df["cep"].astype(str)
                .map(fix_encoding)
                .str.replace(r"[^0-9]", "", regex=True)
            )

        # --------------------------------------------------------
        # Textos gerais
        # --------------------------------------------------------
        for c in ["logradouro", "bairro", "cidade", "uf", "numero"]:
            if c in df.columns:
                df[c] = (
                    df[c]
                    .astype(str)
                    .map(lambda x: fix_encoding(x.strip()))
                    .replace({"nan": "", "None": ""})
                )

        # --------------------------------------------------------
        # UF
        # --------------------------------------------------------
        estados_validos = set(UF_BOUNDS.keys())
        if "uf" in df.columns:
            df["uf"] = df["uf"].str.upper().str.strip()
            uf_invalidas = df.loc[
                ~df["uf"].isin(estados_validos) & df["uf"].ne(""), "uf"
            ].unique()
            if len(uf_invalidas) > 0:
                logger.warning(f"⚠️ UFs inválidas detectadas: {', '.join(uf_invalidas)}")

        # --------------------------------------------------------
        # Cidade
        # --------------------------------------------------------
        if "cidade" in df.columns:
            df["cidade"] = df["cidade"].map(
                lambda x: fix_encoding(str(x)).upper().strip()
            )

        # --------------------------------------------------------
        # Vendas
        # --------------------------------------------------------
        if "pdv_vendas" in df.columns:
            import math

            def normalizar_vendas(valor):
                if pd.isna(valor):
                    return None
                v = fix_encoding(str(valor).strip())
                v = v.replace("R$", "").replace("r$", "").strip()
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
            logger.info(f"ℹ️ {vendas_validas} registros com vendas numéricas válidas.")

        return df


    # ------------------------------------------------------------
    # Filtra apenas colunas relevantes
    # ------------------------------------------------------------
    def filtrar_colunas(self, df: pd.DataFrame) -> pd.DataFrame:
        colunas_base = ["cnpj", "logradouro", "numero", "bairro", "cidade", "uf", "cep"]
        colunas_opcionais = ["pdv_vendas"]
        colunas_presentes = [c for c in (colunas_base + colunas_opcionais) if c in df.columns]
        return df[colunas_presentes].copy()

    # ------------------------------------------------------------
    # Execução principal
    # ------------------------------------------------------------
    def execute(self, input_path: str, sep=";") -> tuple:
        logger.info(f"📄 Lendo arquivo de entrada: {input_path}")

        df = pd.read_csv(input_path, sep=sep, dtype=str, encoding="utf-8", engine="python").fillna("")
        df.columns = [c.strip().lower() for c in df.columns]

        df = self.normalizar_colunas(df)

        # 🔥 converte "número" → numero
        for col in df.columns:
            if col in ("número", "núm", "numéro", "nº"):
                df.rename(columns={col: "numero"}, inplace=True)

        df = self.limpar_valores(df)
        df = self.filtrar_colunas(df)

        # ============================================================
        # 🛠 PRÉ-PROCESSAMENTO REAL DE ENDEREÇOS (NOVO)
        # ============================================================
        def limpar_logradouro(log):
            if not log:
                return ""
            log = log.split(",")[0].strip()
            log = " ".join(log.split())
            return log

        def extrair_numero(logradouro, numero):
            """
            Regra robusta:
            1. Se número explícito no CSV → usa
            2. Se número está no logradouro → extrai
            3. Se estiver no final da string → extrai
            4. Se tiver 'S/N' → retorna ''
            5. Caso contrário → ''
            """
            # 1) número vindo da coluna
            if numero:
                m = re.search(r"(\d+)", str(numero))
                if m:
                    return m.group(1)

            log = str(logradouro).strip()

            # 2) número no logradouro
            m2 = re.search(r"\b(\d+)\b", log)
            if m2:
                return m2.group(1)

            # 3) número no final → ex: "R JOAO DA SILVA 123"
            m3 = re.search(r"(\d+)$", log)
            if m3:
                return m3.group(1)

            # 4) contém "s/n"
            if "s/n" in log.lower() or "sn" in log.lower():
                return ""

            return ""


        def limpar_bairro(bairro, cidade):
            if not bairro:
                return ""

            bairro = bairro.strip()
            cidade = cidade.strip()

            # Remove a cidade quando ela aparece colada ao final
            if cidade and bairro.upper().endswith(" " + cidade.upper()):
                return bairro[: -(len(cidade) + 1)].strip()

            return bairro



        for idx, row in df.iterrows():
            log = row.get("logradouro", "")
            num = row.get("numero", "")
            bairro = row.get("bairro", "")
            cidade = row.get("cidade", "")

            df.at[idx, "logradouro"] = limpar_logradouro(log)
            df.at[idx, "numero"] = extrair_numero(log, num)
            df.at[idx, "bairro"] = limpar_bairro(bairro, cidade)

       
        # Colunas obrigatórias
        colunas_esperadas = ["cnpj", "logradouro", "numero", "cidade", "uf", "cep"]
        faltantes = [col for col in colunas_esperadas if col not in df.columns]
        if faltantes:
            raise ValueError(f"❌ Colunas obrigatórias ausentes: {', '.join(faltantes)}")

        # ============================================================
        # 🏗️ MONTAGEM CORRETA DO ENDEREÇO COMPLETO (INCLUIR CEP)
        # ============================================================

        def montar_endereco(r):
            log = fix_encoding(r["logradouro"]).strip()
            num = fix_encoding(r["numero"]).strip()
            bairro = fix_encoding(str(r.get("bairro", ""))).strip()
            cidade = fix_encoding(r["cidade"]).strip()
            uf = fix_encoding(r["uf"]).strip()
            cep = fix_encoding(r["cep"]).strip()

            # Formata CEP -> 01419002 → 01419-002
            if len(cep) == 8:
                cep_fmt = f"{cep[:5]}-{cep[5:]}"
            else:
                cep_fmt = ""

            # Montagem com fallback para ausência de número
            if num:
                endereco = f"{log} {num}, {bairro}, {cidade} - {uf}"
            else:
                endereco = f"{log}, {bairro}, {cidade} - {uf}"

            # Se CEP válido, adiciona
            if cep_fmt:
                endereco = f"{endereco}, {cep_fmt}"

            # Fecha com Brasil (obrigatório para Nominatim)
            return f"{endereco}, Brasil"


        df["pdv_endereco_completo"] = df.apply(montar_endereco, axis=1)


        # Validação
        df_validos, df_invalidos = self.validator.validar_dados(df, tenant_id=self.tenant_id)
        if df_validos.empty:
            logger.warning(f"⚠️ [{self.tenant_id}] Nenhum PDV válido para geolocalização.")
            return pd.DataFrame(), df_invalidos, 0

        # Cache lookup
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

        # Geocodificação
        if enderecos_novos:
            enderecos_para_geo = [
                df_validos.iloc[i]["pdv_endereco_completo"]
                for i in enderecos_novos
            ]
            resultados_geo = self.geo_service.geocodificar_em_lote(
                enderecos_para_geo,
                tipo="PDV"
            )

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
                        except Exception as e:
                            logger.warning(f"⚠️ Falha ao salvar no cache: {e}")
                    else:
                        df_validos.at[i, "pdv_status"] = "falha"
                else:
                    df_validos.at[i, "pdv_status"] = "falha"

        # UF × coordenadas
        def validar_limites_uf(row):
            if pd.isna(row["pdv_lat"]) or pd.isna(row["pdv_lon"]):
                return "falha_geolocalizacao"
            bounds = UF_BOUNDS.get(row["uf"])
            if not bounds:
                return "uf_invalida"
            if not (
                bounds["lat_min"] <= row["pdv_lat"] <= bounds["lat_max"] and
                bounds["lon_min"] <= row["pdv_lon"] <= bounds["lon_max"]
            ):
                return "coordenadas_fora_limites"
            return "ok"

        df_validos["motivo_invalidade"] = df_validos.apply(validar_limites_uf, axis=1)
        df_invalidos_geo = df_validos[df_validos["motivo_invalidade"] != "ok"]
        df_validos = df_validos[df_validos["motivo_invalidade"] == "ok"]

        df_invalidos_total = pd.concat([df_invalidos, df_invalidos_geo], ignore_index=True)

        # Inserção
        df_validos["tenant_id"] = self.tenant_id
        df_validos["input_id"] = self.input_id
        df_validos["descricao"] = self.descricao

        campos_validos = PDV.__init__.__code__.co_varnames[1:]
        df_para_inserir = df_validos[[c for c in df_validos.columns if c in campos_validos]]

        pdvs = [PDV(**row) for row in df_para_inserir.to_dict(orient="records")]

        try:
            inseridos = self.writer.inserir_pdvs(pdvs)
        except Exception as e:
            logger.error(f"❌ Falha ao inserir PDVs no banco: {e}", exc_info=True)
            inseridos = 0

        total_falhas = df_validos["status_geolocalizacao"].eq("falha").sum()
        logger.info(f"✅ [{self.tenant_id}] {len(df_validos)} válidos / {len(df_invalidos_total)} inválidos.")
        logger.info(f"💾 [{self.tenant_id}] {inseridos} PDVs inseridos (input_id={self.input_id}).")
        logger.info(f"⚠️ {total_falhas} PDVs não geocodificados.")

        self.geo_service.exibir_resumo_logs()

        return df_validos, df_invalidos_total, inseridos

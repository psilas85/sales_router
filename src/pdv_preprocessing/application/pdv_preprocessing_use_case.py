#sales_router/src/pdv_preprocessing/application/pdv_preprocessing_use_case.py

# ============================================================
# 📦 src/pdv_preprocessing/application/pdv_preprocessing_use_case.py
# ============================================================

import os
import sys
import pandas as pd
import unicodedata
import re
import time
import uuid
import json
from loguru import logger
from rq import get_current_job
from pdv_preprocessing.entities.pdv_entity import PDV
from pdv_preprocessing.domain.pdv_validation_service import PDVValidationService
from pdv_preprocessing.domain.geolocation_service import GeolocationService
from pdv_preprocessing.config.uf_bounds import UF_BOUNDS
from pdv_preprocessing.domain.utils_texto import fix_encoding


# ============================================================
# 🔵 FUNÇÃO ÚNICA DE PROGRESSO (0–99%)
# ============================================================
def atualizar_progresso(atual, total, passo_min, passo_max, step):
    """
    Converte progresso local em progresso global.
    A etapa ocupa o intervalo [passo_min, passo_max].
    """
    job = get_current_job()
    if not job:
        return

    # Cálculo do percentual global
    if total <= 0:
        pct_global = passo_min
    else:
        pct_local = atual / total
        pct_global = passo_min + int(pct_local * (passo_max - passo_min))

    # Nunca permitir 100% aqui
    pct_global = max(1, min(99, pct_global))

    # Atualiza meta do Redis para o /progress
    job.meta.update({"progress": pct_global, "step": step})
    job.save_meta()

    # Emite no stdout se estiver em subprocesso (CLI)
    print(json.dumps({
        "event": "progress",
        "pct": pct_global,
        "step": step
    }))
    sys.stdout.flush()


# ============================================================
# 📦 CLASSE PRINCIPAL
# ============================================================
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
    def normalizar_colunas(self, df):
        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.normalize("NFC")
        )
        return df

    # ------------------------------------------------------------
    def limpar_valores(self, df: pd.DataFrame) -> pd.DataFrame:

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

        def normalizar_cnpj(valor):
            if pd.isna(valor) or str(valor).strip() == "":
                return None
            v = fix_encoding(str(valor).strip())
            if re.match(r"^\d+,\d+E\+\d+$", v):
                v = v.replace(",", ".")
            try:
                if "E+" in v or "e+" in v:
                    v = f"{float(v):.0f}"
            except:
                pass
            return re.sub(r"[^0-9]", "", v)

        if "cnpj" not in df.columns:
            df["cnpj"] = ""

        df["cnpj"] = df["cnpj"].apply(normalizar_cnpj)

        for idx, val in df["cnpj"].items():
            if val in (None, "", "nan"):
                df.at[idx, "cnpj"] = gerar_cnpj_valido(idx)

        if "cep" in df.columns:
            df["cep"] = (
                df["cep"].astype(str)
                .map(fix_encoding)
                .str.replace(r"[^0-9]", "", regex=True)
            )

        for c in ["logradouro", "bairro", "cidade", "uf", "numero"]:
            if c in df.columns:
                df[c] = (
                    df[c]
                    .astype(str)
                    .map(lambda x: fix_encoding(x.strip()))
                    .replace({"nan": "", "None": ""})
                )

        estados_validos = set(UF_BOUNDS.keys())
        if "uf" in df.columns:
            df["uf"] = df["uf"].str.upper().str.strip()
            uf_invalidas = df.loc[
                ~df["uf"].isin(estados_validos) & df["uf"].ne(""), "uf"
            ].unique()
            if len(uf_invalidas) > 0:
                logger.warning(f"⚠️ UFs inválidas: {', '.join(uf_invalidas)}")

        if "cidade" in df.columns:
            df["cidade"] = df["cidade"].map(lambda x: fix_encoding(str(x)).upper().strip())

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
                except:
                    return None

            df["pdv_vendas"] = df["pdv_vendas"].apply(normalizar_vendas)

        return df

    # ------------------------------------------------------------
    def filtrar_colunas(self, df):
        colunas_base = ["cnpj", "logradouro", "numero", "bairro", "cidade", "uf", "cep"]
        colunas_opcionais = ["pdv_vendas"]
        colunas_presentes = [c for c in (colunas_base + colunas_opcionais) if c in df.columns]
        return df[colunas_presentes].copy()

    # ------------------------------------------------------------
    # EXECUTE COM PROGRESSO
    # ------------------------------------------------------------
    def execute(self, input_path: str, sep=";"):

        logger.info(f"📄 Lendo arquivo: {input_path}")

        df = pd.read_csv(input_path, sep=sep, dtype=str, encoding="utf-8", engine="python").fillna("")
        df.columns = [c.strip().lower() for c in df.columns]

        total_linhas = len(df)
        atualizar_progresso(1, total_linhas, 0, 5, "Lendo arquivo")

        df = self.normalizar_colunas(df)

        # Etapa 1 — limpeza
        df = self.limpar_valores(df)
        atualizar_progresso(1, 1, 5, 15, "Normalizando valores")

        df = self.filtrar_colunas(df)
        atualizar_progresso(1, 1, 15, 30, "Filtrando colunas")

        # Padronização linha a linha
        def limpar_logradouro(log):
            if not log:
                return ""
            return " ".join(log.split(",")[0].strip().split())

        def extrair_numero(logradouro, numero):
            if numero:
                m = re.search(r"(\d+)", str(numero))
                if m:
                    return m.group(1)

            log = str(logradouro).strip()
            m2 = re.search(r"\b(\d+)\b", log)
            if m2:
                return m2.group(1)
            m3 = re.search(r"(\d+)$", log)
            if m3:
                return m3.group(1)
            if "s/n" in log.lower() or "sn" in log.lower():
                return ""
            return ""

        def limpar_bairro(bairro, cidade):
            if not bairro:
                return ""
            bairro = bairro.strip()
            cidade = cidade.strip()
            if cidade and bairro.upper().endswith(" " + cidade.upper()):
                return bairro[: -(len(cidade) + 1)].strip()
            return bairro

        for i, row in df.iterrows():
            df.at[i, "logradouro"] = limpar_logradouro(row.get("logradouro", ""))
            df.at[i, "numero"] = extrair_numero(row.get("logradouro", ""), row.get("numero", ""))
            df.at[i, "bairro"] = limpar_bairro(row.get("bairro", ""), row.get("cidade", ""))

            if i % 300 == 0:
                atualizar_progresso(i, total_linhas, 30, 45, "Limpando linhas")

        colunas_esperadas = ["cnpj", "logradouro", "numero", "cidade", "uf", "cep"]
        faltantes = [c for c in colunas_esperadas if c not in df.columns]
        if faltantes:
            raise ValueError(f"❌ Colunas ausentes: {', '.join(faltantes)}")

        def montar_endereco(r):
            log = fix_encoding(r["logradouro"]).strip()
            num = fix_encoding(r["numero"]).strip()
            bairro = fix_encoding(str(r.get("bairro", ""))).strip()
            cidade = fix_encoding(r["cidade"]).strip()
            uf = fix_encoding(r["uf"]).strip()
            cep = fix_encoding(r["cep"]).strip()

            if len(cep) == 8:
                cep_fmt = f"{cep[:5]}-{cep[5:]}"
            else:
                cep_fmt = ""

            if num:
                base = f"{log} {num}, {bairro}, {cidade} - {uf}"
            else:
                base = f"{log}, {bairro}, {cidade} - {uf}"

            if cep_fmt:
                base = f"{base}, {cep_fmt}"

            return f"{base}, Brasil"

        df["pdv_endereco_completo"] = df.apply(montar_endereco, axis=1)
        atualizar_progresso(1, 1, 45, 55, "Montando endereços")

        # Validação
        df_validos, df_invalidos = self.validator.validar_dados(df, tenant_id=self.tenant_id)

        if df_validos.empty:
            return pd.DataFrame(), df_invalidos, 0

        # Cache
        end_norm_list = df_validos["pdv_endereco_completo"].str.strip().str.lower().tolist()
        cache_db = self.reader.buscar_enderecos_cache(end_norm_list)

        df_validos["pdv_lat"] = None
        df_validos["pdv_lon"] = None
        df_validos["status_geolocalizacao"] = None

        enderecos_novos = []

        for i, row in df_validos.iterrows():
            end_norm = row["pdv_endereco_completo"].strip().lower()
            if end_norm in cache_db:
                lat, lon = cache_db[end_norm]
                df_validos.at[i, "pdv_lat"] = lat
                df_validos.at[i, "pdv_lon"] = lon
                df_validos.at[i, "status_geolocalizacao"] = "cache_db"
            else:
                enderecos_novos.append(i)

            if i % 300 == 0:
                atualizar_progresso(i, len(df_validos), 55, 65, "Consultando cache")

        # Geocodificação
        if enderecos_novos:
            atualizar_progresso(1, 1, 65, 85, "Geocodificando")

            enderecos_para_geo = [
                df_validos.iloc[i]["pdv_endereco_completo"]
                for i in enderecos_novos
            ]

            resultados_geo = self.geo_service.geocodificar_em_lote(
                enderecos_para_geo, tipo="PDV"
            )

            for k, idx in enumerate(enderecos_novos):
                endereco = df_validos.iloc[idx]["pdv_endereco_completo"]
                if endereco in resultados_geo:
                    lat, lon, origem = resultados_geo[endereco]
                    if lat is not None and lon is not None:
                        df_validos.at[idx, "pdv_lat"] = lat
                        df_validos.at[idx, "pdv_lon"] = lon
                        df_validos.at[idx, "status_geolocalizacao"] = origem
                        try:
                            self.writer.inserir_localizacao(endereco, lat, lon)
                        except:
                            pass

                if k % 20 == 0:
                    atualizar_progresso(k, len(enderecos_novos), 65, 85, "Geocodificando")

        # UF bounds
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

        atualizar_progresso(1, 1, 85, 95, "Inserindo no banco")

        # Inserção
        df_validos["tenant_id"] = self.tenant_id
        df_validos["input_id"] = self.input_id
        df_validos["descricao"] = self.descricao

        campos_validos = PDV.__init__.__code__.co_varnames[1:]
        df_insert = df_validos[[c for c in df_validos.columns if c in campos_validos]]

        pdvs = [PDV(**row) for row in df_insert.to_dict(orient="records")]

        try:
            inseridos = self.writer.inserir_pdvs(pdvs)
        except Exception as e:
            logger.error(f"❌ Erro ao inserir PDVs: {e}", exc_info=True)
            inseridos = 0

        atualizar_progresso(1, 1, 95, 99, "Finalizando")

        return df_validos, df_invalidos_total, inseridos

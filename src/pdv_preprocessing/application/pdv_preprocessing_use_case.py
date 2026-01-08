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

# ✅ NOVO: normalizador (mesma regra do pipeline)
from pdv_preprocessing.domain.address_normalizer import (
    normalize_base,
    normalize_for_cache,
)


def extrair_logradouro_numero(logradouro_raw: str):
    """
    Extrai número apenas se estiver no FINAL do logradouro.
    Evita 'Rua 25 de Março', 'Av 9 de Julho'.
    """
    if not logradouro_raw:
        return "", ""

    logradouro_raw = logradouro_raw.strip()

    m = re.search(r"^(.*?)(?:\s*,?\s*)(\d{1,6})$", logradouro_raw)
    if m:
        return m.group(1).strip(), m.group(2)

    return logradouro_raw, ""


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
    def __init__(self, reader, writer, tenant_id, input_id=None, descricao=None, usar_google=True):
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
        def normalize_col(c: str) -> str:
            c = c.strip().lower()
            c = unicodedata.normalize("NFKD", c)
            c = "".join(ch for ch in c if not unicodedata.combining(ch))
            return c

        df.columns = [normalize_col(c) for c in df.columns]
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
                pesos_1 = [5, 4, 3, 2, 9, 8, 7, 6, 5, 4, 3, 2]
                pesos_2 = [6] + pesos_1
                soma1 = sum(int(a) * b for a, b in zip(cnpj, pesos_1))
                d1 = 11 - (soma1 % 11)
                d1 = d1 if d1 < 10 else 0
                soma2 = sum(int(a) * b for a, b in zip(cnpj + str(d1), pesos_2))
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

                v = fix_encoding(str(valor)).strip()

                # remove moeda
                v = v.replace("R$", "").replace("r$", "").strip()

                # CASO 1: formato brasileiro explícito
                if "," in v:
                    v = v.replace(".", "").replace(",", ".")
                else:
                    # CASO 2: lixo numérico do Excel
                    try:
                        num = float(v)
                        if num > 1_000_000_000:
                            return None
                        return num
                    except:
                        return None

                v = re.sub(r"[^0-9.]", "", v)

                try:
                    num = float(v)
                    if num > 1_000_000_000:
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
    # EXECUTE COM PROGRESSO + HISTÓRICO + TRATAMENTO COMPLETO
    # ------------------------------------------------------------
    def execute(self, input_path: str, sep=";"):

        job = get_current_job()
        job_id = job.id if job else str(uuid.uuid4())

        try:
            logger.info(f"📄 Lendo arquivo: {input_path}")

            ext = os.path.splitext(input_path)[1].lower()

            # ============================================================
            # 📥 LEITURA DO ARQUIVO (XLSX ou CSV)
            # ============================================================
            if ext in [".xlsx", ".xls"]:
                df = pd.read_excel(
                    input_path,
                    dtype=str,
                    engine="openpyxl"
                ).fillna("")
            else:
                df = pd.read_csv(
                    input_path,
                    sep=sep,
                    dtype=str,
                    encoding="utf-8",
                    engine="python"
                ).fillna("")

            total_linhas = len(df)
            atualizar_progresso(1, total_linhas, 0, 5, "Lendo arquivo")

            # ============================================================
            # HISTÓRICO — INÍCIO
            # ============================================================
            self.writer.salvar_historico_pdv_job(
                tenant_id=self.tenant_id,
                job_id=job_id,
                arquivo=os.path.basename(input_path),
                status="running",
                total_processados=total_linhas,
                descricao=self.descricao,
                input_id=self.input_id
            )

            # ============================================================
            # NORMALIZAÇÃO BÁSICA
            # ============================================================
            df = self.normalizar_colunas(df)
            df = self.limpar_valores(df)
            atualizar_progresso(1, 1, 5, 15, "Normalizando valores")

            df = self.filtrar_colunas(df)
            atualizar_progresso(1, 1, 15, 30, "Filtrando colunas")

            # ============================================================
            # NORMALIZA LOGRADOURO / NÚMERO / BAIRRO
            # ============================================================
            def normalizar_logradouro(logradouro_raw: str):
                if not logradouro_raw:
                    return ""
                log = str(logradouro_raw).strip()
                log = log.split(",")[0].strip()
                return " ".join(log.split())

            def normalizar_numero(numero_raw: str):
                num = str(numero_raw).strip().upper()
                if num in ("", "0", "SN", "S/N", "SEM NUMERO", "SEM NÚMERO"):
                    return ""
                return num

            def limpar_bairro(bairro, cidade):
                if not bairro:
                    return ""
                bairro = str(bairro).strip()
                cidade = str(cidade).strip()
                if cidade and bairro.upper().endswith(" " + cidade.upper()):
                    return bairro[:-(len(cidade) + 1)].strip()
                return bairro

            for i, row in df.iterrows():
                log_raw = row.get("logradouro", "")
                num_raw = row.get("numero", "")

                log_extraido, num_extraido = extrair_logradouro_numero(log_raw)
                numero_final = num_raw if str(num_raw).strip() else num_extraido

                df.at[i, "logradouro"] = normalizar_logradouro(log_extraido)
                df.at[i, "numero"] = normalizar_numero(numero_final)
                df.at[i, "bairro"] = limpar_bairro(
                    row.get("bairro", ""), row.get("cidade", "")
                )

                if i % 300 == 0:
                    atualizar_progresso(i, total_linhas, 30, 45, "Normalizando endereços")

            # ============================================================
            # VALIDAÇÃO DE COLUNAS
            # ============================================================
            colunas_esperadas = ["cnpj", "logradouro", "numero", "cidade", "uf", "cep"]
            faltantes = [c for c in colunas_esperadas if c not in df.columns]
            if faltantes:
                raise ValueError(f"❌ Colunas ausentes: {', '.join(faltantes)}")

            # ============================================================
            # ENDEREÇO COMPLETO
            # ============================================================
            def montar_endereco(r):
                log = fix_encoding(r["logradouro"]).strip()
                num = fix_encoding(r["numero"]).strip()
                bairro = fix_encoding(str(r.get("bairro", ""))).strip()
                cidade = fix_encoding(r["cidade"]).strip()
                uf = fix_encoding(r["uf"]).strip()
                cep = fix_encoding(r["cep"]).strip()

                cep_fmt = f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else ""

                base = (
                    f"{log} {num}, {bairro}, {cidade} - {uf}"
                    if num else
                    f"{log}, {bairro}, {cidade} - {uf}"
                )

                if cep_fmt:
                    base = f"{base}, {cep_fmt}"

                return f"{base}, Brasil"

            df["pdv_endereco_completo"] = df.apply(montar_endereco, axis=1)

            df["endereco_cache_key"] = df["pdv_endereco_completo"].apply(
                lambda e: normalize_for_cache(normalize_base(e)) if e else ""
            )

            atualizar_progresso(1, 1, 45, 55, "Montando endereços")

            # ============================================================
            # VALIDAÇÃO DE DADOS
            # ============================================================
            df_validos, df_invalidos = self.validator.validar_dados(
                df, tenant_id=self.tenant_id
            )

            if df_validos.empty:
                self.writer.salvar_historico_pdv_job(
                    tenant_id=self.tenant_id,
                    job_id=job_id,
                    arquivo=os.path.basename(input_path),
                    status="success",
                    total_processados=total_linhas,
                    validos=0,
                    invalidos=len(df_invalidos),
                    descricao=self.descricao,
                    input_id=self.input_id
                )
                return pd.DataFrame(), df_invalidos, 0

            # ============================================================
            # GEOCODIFICAÇÃO
            # ============================================================
            df_validos["pdv_lat"] = None
            df_validos["pdv_lon"] = None
            df_validos["status_geolocalizacao"] = None

            enderecos = df_validos["pdv_endereco_completo"].tolist()
            atualizar_progresso(1, 1, 55, 85, "Geocodificando")

            resultados_geo = self.geo_service.geocodificar_em_lote(enderecos)

            for i, _ in enumerate(enderecos):
                lat, lon, origem = resultados_geo.get(i, (None, None, "falha"))
                idx = df_validos.index[i]

                df_validos.at[idx, "pdv_lat"] = lat
                df_validos.at[idx, "pdv_lon"] = lon
                df_validos.at[idx, "status_geolocalizacao"] = origem

                if i % 200 == 0:
                    atualizar_progresso(i, len(enderecos), 55, 85, "Geocodificando")

            # ============================================================
            # UF BOUNDS
            # ============================================================
            def validar_limites_uf(row):
                if pd.isna(row["pdv_lat"]) or pd.isna(row["pdv_lon"]):
                    return "falha_geolocalizacao"
                bounds = UF_BOUNDS.get(row["uf"])
                if not bounds:
                    return "uf_invalida"
                if not (
                    bounds["lat_min"] <= row["pdv_lat"] <= bounds["lat_max"]
                    and bounds["lon_min"] <= row["pdv_lon"] <= bounds["lon_max"]
                ):
                    return "coordenadas_fora_limites"
                return "ok"

            df_validos["motivo_invalidade"] = df_validos.apply(validar_limites_uf, axis=1)

            df_invalidos_geo = df_validos[df_validos["motivo_invalidade"] != "ok"]
            df_validos = df_validos[df_validos["motivo_invalidade"] == "ok"]

            df_invalidos_total = pd.concat(
                [df_invalidos, df_invalidos_geo], ignore_index=True
            )

            # ============================================================
            # INSERÇÃO NO BANCO
            # ============================================================
            atualizar_progresso(1, 1, 85, 95, "Inserindo no banco")

            df_validos["tenant_id"] = self.tenant_id
            df_validos["input_id"] = self.input_id
            df_validos["descricao"] = self.descricao

            # BLINDAGEM FINAL pdv_vendas (TEM QUE SER ANTES)
            if "pdv_vendas" in df_validos.columns:
                df_validos["pdv_vendas"] = pd.to_numeric(
                    df_validos["pdv_vendas"],
                    errors="coerce"
                )

            campos_validos = PDV.__init__.__code__.co_varnames[1:]
            df_insert = df_validos[[c for c in df_validos.columns if c in campos_validos]]
            pdvs = [PDV(**row) for row in df_insert.to_dict(orient="records")]
            inseridos = self.writer.inserir_pdvs(pdvs)

            atualizar_progresso(1, 1, 95, 99, "Finalizando")

            # ============================================================
            # HISTÓRICO — FINAL
            # ============================================================
            self.writer.salvar_historico_pdv_job(
                tenant_id=self.tenant_id,
                job_id=job_id,
                arquivo=os.path.basename(input_path),
                status="success",
                total_processados=total_linhas,
                validos=len(df_validos),
                invalidos=len(df_invalidos_total),
                inseridos=inseridos,
                descricao=self.descricao,
                input_id=self.input_id
            )

            return df_validos, df_invalidos_total, inseridos

        except Exception as e:
            logger.error("❌ Erro no processamento PDV", exc_info=True)

            self.writer.salvar_historico_pdv_job(
                tenant_id=self.tenant_id,
                job_id=job_id,
                arquivo=os.path.basename(input_path),
                status="error",
                total_processados=0,
                mensagem=str(e),
                descricao=self.descricao,
                input_id=self.input_id
            )
            raise

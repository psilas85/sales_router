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
from pdv_preprocessing.domain.geolocation_service import GeolocationService as LocalGeolocationService
from pdv_preprocessing.config.uf_bounds import UF_BOUNDS
from pdv_preprocessing.domain.utils_texto import fix_encoding
from pdv_preprocessing.infrastructure.geocoding_engine_client import GeocodingEngineClient

# ✅ NOVO: normalizador (mesma regra do pipeline)
from pdv_preprocessing.domain.address_normalizer import (
    normalize_base,
    normalize_for_cache,
)
from pdv_preprocessing.infrastructure.geocoding_engine_client import (
    GeocodingEngineClient,
    STATUS_FALHA_INTEGRACAO,
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
    STATUS_INPUT_GEOCODING_INVALIDO = "falha_input_geocoding"

    def __init__(self, reader, writer, tenant_id, input_id=None, descricao=None, usar_google=True):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.input_id = input_id or str(uuid.uuid4())
        self.descricao = descricao or "PDV Importado"
        self.usar_google = usar_google

        self.validator = PDVValidationService(db_reader=reader)
        self.geo_client = GeocodingEngineClient()
        self.geo_service = None

    def _normalizar_motivo_geocoding(self, status_geolocalizacao: str | None) -> str:
        status = str(status_geolocalizacao or "").strip().lower()

        mapping = {
            self.STATUS_INPUT_GEOCODING_INVALIDO: self.STATUS_INPUT_GEOCODING_INVALIDO,
            STATUS_FALHA_INTEGRACAO: STATUS_FALHA_INTEGRACAO,
            "falha": "falha_geocoding",
            "invalid_input": self.STATUS_INPUT_GEOCODING_INVALIDO,
            "cidade_invalida": "cidade_invalida",
            "fora_municipio": "fora_municipio",
            "fallback_falhou": "fallback_falhou",
            "falha_total": "falha_geocoding",
        }

        return mapping.get(status, status or "falha_geocoding")

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
    def _preparar_payload_geocoding(self, df_validos: pd.DataFrame):
        payload = []
        resultados_locais = {}

        for idx, row in df_validos.iterrows():
            logradouro = str(row.get("logradouro", "") or "").strip()
            cidade = str(row.get("cidade", "") or "").strip()
            uf = str(row.get("uf", "") or "").strip()
            address = str(row.get("pdv_endereco_completo", "") or "").strip()

            if not logradouro or not cidade or len(uf) != 2 or not address:
                resultados_locais[int(idx)] = (None, None, self.STATUS_INPUT_GEOCODING_INVALIDO)
                continue

            payload.append({
                "id": int(idx),
                "address": address,
                "logradouro": logradouro,
                "numero": str(row.get("numero", "") or "").strip(),
                "bairro": str(row.get("bairro", "") or "").strip(),
                "cidade": cidade,
                "uf": uf,
                "cep": str(row.get("cep", "") or "").strip(),
            })

        return payload, resultados_locais

    # ------------------------------------------------------------
    def _geocodificar_pdvs(self, df_validos: pd.DataFrame):
        payload, resultados_locais = self._preparar_payload_geocoding(df_validos)

        if not payload:
            return resultados_locais

        if self.geo_client.enabled:
            try:
                def atualizar_progresso_geocoding_remoto(progress: int, step: str | None):
                    atualizar_progresso(
                        max(1, progress),
                        100,
                        58,
                        82,
                        step or "Geocodificando",
                    )

                atualizar_progresso(1, 1, 55, 58, "Preparando geocodificacao")

                resultados_remotos = self.geo_client.geocode_pdv_batch_job(
                    payload,
                    on_progress=atualizar_progresso_geocoding_remoto,
                )
                resultados_remotos.update(resultados_locais)
                return resultados_remotos
            except Exception as e:
                if os.getenv("GEOCODING_ENGINE_ALLOW_LOCAL_FALLBACK", "false").lower() == "true":
                    logger.warning(
                        f"[GEOCODING_ENGINE][FALLBACK_LOCAL] erro={e}",
                        exc_info=True,
                    )
                else:
                    logger.error(
                        f"[GEOCODING_ENGINE][JOB_ERRO] erro={e}",
                        exc_info=True,
                    )
                    raise
        else:
            if os.getenv("GEOCODING_ENGINE_ALLOW_LOCAL_FALLBACK", "false").lower() == "true":
                logger.warning(
                    "[GEOCODING_ENGINE][DISABLED] GEOCODING_ENGINE_URL não configurada; "
                    "usando geocoder local"
                )
            else:
                raise RuntimeError(
                    "GEOCODING_ENGINE_URL não configurada; "
                    "pdv_preprocessing requer geocoding_engine para validação completa"
                )

        if self.geo_service is None:
            self.geo_service = LocalGeolocationService(
                self.reader,
                self.writer,
                usar_google=self.usar_google,
            )

        enderecos = df_validos["pdv_endereco_completo"].tolist()
        resultados = self.geo_service.geocodificar_em_lote(enderecos)

        resultados_finais = {
            int(df_validos.index[pos]): resultado
            for pos, resultado in resultados.items()
        }
        resultados_finais.update(resultados_locais)
        return resultados_finais

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

            df["endereco_cache_key"] = df.apply(
                lambda r: normalize_for_cache(
                    normalize_base(
                        f"{r.get('logradouro', '')} {r.get('numero', '')}, "
                        f"{r.get('cidade', '')} - {r.get('uf', '')}"
                    )
                ),
                axis=1,
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

            atualizar_progresso(1, 1, 55, 58, "Preparando geocodificacao")

            resultados_geo = self._geocodificar_pdvs(df_validos)

            total_geo = len(df_validos)
            atualizar_progresso(1, 1, 82, 85, "Consolidando resultados da geocodificacao")

            for i, idx in enumerate(df_validos.index):
                lat, lon, origem = resultados_geo.get(int(idx), (None, None, "falha"))

                df_validos.at[idx, "pdv_lat"] = lat
                df_validos.at[idx, "pdv_lon"] = lon
                df_validos.at[idx, "status_geolocalizacao"] = origem

                if i % 100 == 0:
                    atualizar_progresso(
                        i + 1,
                        total_geo,
                        82,
                        85,
                        "Consolidando resultados da geocodificacao",
                    )

            status_counts = (
                df_validos["status_geolocalizacao"]
                .fillna("falha")
                .astype(str)
                .value_counts()
                .to_dict()
            )

            # ============================================================
            # UF BOUNDS
            # ============================================================
            def auditar_limites_uf(row):
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

            df_validos["status_auditoria_geo"] = df_validos.apply(auditar_limites_uf, axis=1)

            df_invalidos_geo = df_validos[
                df_validos["status_auditoria_geo"] == "falha_geolocalizacao"
            ].copy()
            if not df_invalidos_geo.empty:
                df_invalidos_geo["motivo_invalidade"] = df_invalidos_geo[
                    "status_geolocalizacao"
                ].apply(self._normalizar_motivo_geocoding)

            df_validos = df_validos[
                df_validos["status_auditoria_geo"] != "falha_geolocalizacao"
            ].copy()

            df_auditoria_geo = df_validos[
                df_validos["status_auditoria_geo"].isin(["uf_invalida", "coordenadas_fora_limites"])
            ]
            if not df_auditoria_geo.empty:
                logger.warning(
                    "[PDV_PREPROCESSING][AUDITORIA_GEO] "
                    f"inconsistencias={len(df_auditoria_geo)}"
                )

            metricas_integracao = {
                "payload_enviado": sum(1 for v in resultados_geo.values() if v[2] != self.STATUS_INPUT_GEOCODING_INVALIDO),
                "payload_barrado": sum(1 for v in resultados_geo.values() if v[2] == self.STATUS_INPUT_GEOCODING_INVALIDO),
                "falha_integracao": sum(1 for v in resultados_geo.values() if v[2] == STATUS_FALHA_INTEGRACAO),
                "falha_geocodificacao": sum(1 for v in resultados_geo.values() if v[2] == "falha"),
                "auditoria_geo": len(df_auditoria_geo),
            }

            logger.info(
                "[PDV_PREPROCESSING][GEOCODING_RESUMO] "
                f"status={status_counts} metricas={metricas_integracao}"
            )

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
                mensagem=(
                    "geocoding="
                    f"enviado:{metricas_integracao['payload_enviado']}|"
                    f"barrado:{metricas_integracao['payload_barrado']}|"
                    f"falha_integracao:{metricas_integracao['falha_integracao']}|"
                    f"falha_geocodificacao:{metricas_integracao['falha_geocodificacao']}|"
                    f"auditoria_geo:{metricas_integracao['auditoria_geo']}"
                ),
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

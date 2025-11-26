#sales_router/src/pdv_preprocessing/cep_area_geocoding/application/area_geocoding_use_case.py
# ============================================================
# 📦 src/pdv_preprocessing/cep_area_geocoding/application/area_geocoding_use_case.py
# ============================================================

import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from random import uniform
from loguru import logger

from pdv_preprocessing.cep_area_geocoding.domain.area_validator import AreaValidator

# remover warnings pandas
import warnings
from pandas.errors import SettingWithCopyWarning
warnings.simplefilter("ignore", UserWarning)
warnings.simplefilter("ignore", FutureWarning)
warnings.simplefilter("ignore", SettingWithCopyWarning)


class AreaGeocodingUseCase:

    def __init__(self, reader, writer, geo, max_workers=12):
        self.reader = reader
        self.writer = writer
        self.geo = geo
        self.validador = AreaValidator()
        self.max_workers = max_workers

    # ============================================================
    # loader — versão robusta para clientes_total / clientes_target
    # ============================================================
    def _carregar_planilha(self, arquivo):
        ext = arquivo.lower()

        # ------------------------------------------------------------
        # 1. Carregar SEM tipar (para evitar erros de conversão)
        # ------------------------------------------------------------
        try:
            if ext.endswith(".xlsx") or ext.endswith(".xls"):
                logger.info(f"📄 Carregando Excel: {arquivo}")
                df = pd.read_excel(arquivo, dtype=str).fillna("")

            else:
                logger.info(f"📄 Carregando CSV: {arquivo}")
                df = pd.read_csv(arquivo, dtype=str, encoding="utf-8").fillna("")

        except UnicodeDecodeError:
            logger.warning(f"⚠️ CSV latin-1 detectado: {arquivo}")
            df = pd.read_csv(arquivo, dtype=str, encoding="latin-1").fillna("")

        # ------------------------------------------------------------
        # 2. Limpeza segura das colunas numéricas
        # ------------------------------------------------------------
        cols_numericas = ["clientes_total", "clientes_target"]

        for col in cols_numericas:
            if col in df.columns:
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace(r"[^\d]", "", regex=True)   # remove tudo que não é dígito
                )

                # tratar vazio
                df[col] = df[col].apply(lambda x: x if x.isdigit() else "0")

                df[col] = df[col].astype(int)

        # ------------------------------------------------------------
        # 3. Conversão segura para int
        # ------------------------------------------------------------
        for col in cols_numericas:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0).astype(int)

        return df

    # ============================================================
    # ✨ WORKER MULTI-THREAD — corrigido p/ enviar clientes_total/target
    # ============================================================
    def _processar_cep(self, tenant_id, input_id, descricao, row):
        ini = time.time()

        cep = row["cep"].strip()
        bairro = row["bairro"].strip()
        cidade = row["cidade"].strip()
        uf = row["uf"].strip()

        # NOVO: captura valores do XLSX
        clientes_total = int(row.get("clientes_total", 0) or 0)
        clientes_target = int(row.get("clientes_target", 0) or 0)

        if bairro:
            endereco_key = f"{bairro}, {cidade} - {uf}, Brasil"
        else:
            endereco_key = f"{cidade} - {uf}, Brasil"

        # geolocalização
        lat, lon, origem = self.geo.buscar(
            tenant_id, cep, endereco_key, bairro, cidade, uf, input_id=input_id
        )

        dur = round((time.time() - ini) * 1000)

        logger.info(
            f"🧭 [{input_id}] {cep} | {bairro}-{cidade}/{uf} "
            f"| origem={origem} | latlon=({lat},{lon}) | {dur}ms"
        )

        if lat is None or lon is None:
            logger.warning(f"⚠️ Falhou → CEP={cep} | {bairro}-{cidade}/{uf}")
            return None

        # jitter leve para não concentrar pontos
        lat += uniform(-0.001, 0.001)
        lon += uniform(-0.001, 0.001)

        # salvar em cep_bairro_cache
        self.writer.salvar_cache_bairro(
            tenant_id, cep, bairro, cidade, uf, endereco_key, lat, lon, origem
        )

        # salvar marketplace_cep COM OS VALORES CORRETOS
        self.writer.salvar_marketplace_cep(
            tenant_id,
            input_id,
            descricao,
            cep,
            bairro,
            cidade,
            uf,
            clientes_total,
            clientes_target,
            lat,
            lon,
            origem
        )

        return (cep, bairro, cidade, uf, clientes_total, clientes_target, lat, lon, origem)

    # ============================================================
    # PROCESSAMENTO PRINCIPAL
    # ============================================================
    def processar(self, tenant_id, input_id, descricao, arquivo):

        logger.info(f"🚀 Iniciando geocodificação | input_id={input_id} | tenant={tenant_id}")

        df = self._carregar_planilha(arquivo)
        df = self.validador.validar(df)

        df["cep"] = (
            df["cep"]
            .astype(str)
            .str.replace(r"[^\d]", "", regex=True)  # remove tudo exceto dígitos
            .str.zfill(8)                           # garante 8 dígitos com zero à esquerda
        )


        # remover duplicados
        before = len(df)
        df = df.drop_duplicates(subset=["cep"])
        removed = before - len(df)
        if removed > 0:
            logger.info(f"🧹 Removidos {removed} duplicados")

        total = len(df)
        logger.info(f"📌 {total} CEPs únicos após limpeza")

        # ============================================================
        # verificar cache prévio do BD
        # ============================================================
        ceps = df["cep"].tolist()
        cache_db = self.reader.buscar_lista_cache_bairro(tenant_id, ceps)

        ceps_cache_db = set(cache_db.keys())
        missing = df[~df["cep"].isin(ceps_cache_db)]

        logger.info(f"🗄️ Cache DB = {len(ceps_cache_db)}")
        logger.info(f"🔎 Missing = {len(missing)}")

        resultados = []

        # ============================================================
        # MULTI-THREAD — somente MISSING
        # ============================================================
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = {
                executor.submit(
                    self._processar_cep,
                    tenant_id,
                    input_id,
                    descricao,
                    row,
                ): row["cep"]
                for _, row in missing.iterrows()
            }

            for future in as_completed(futures):
                cep = futures[future]
                try:
                    result = future.result()
                    if result:
                        resultados.append(result)
                except Exception as e:
                    logger.error(f"❌ Erro thread CEP={cep}: {e}")

       # ============================================================
        # adicionar resultados vindos do cache — coordenadas do cache,
        # clientes_total e clientes_target SEMPRE do input
        # ============================================================
        for cep, (lat, lon, origem) in cache_db.items():

            # SEMPRE pegar os valores do input
            row = df.loc[df["cep"] == cep].iloc[0]

            clientes_total = int(row.get("clientes_total", 0) or 0)
            clientes_target = int(row.get("clientes_target", 0) or 0)

            self.writer.salvar_marketplace_cep(
                tenant_id,
                input_id,
                descricao,
                cep,
                row["bairro"],
                row["cidade"],
                row["uf"],
                clientes_total,
                clientes_target,
                lat,
                lon,
                origem
            )

            resultados.append(
                (
                    cep,
                    row["bairro"],
                    row["cidade"],
                    row["uf"],
                    clientes_total,
                    clientes_target,
                    lat,
                    lon,
                    origem
                )
            )


        logger.success(
            f"🎯 COMPLETO | input_id={input_id} → Total={len(resultados)} "
            f"(Cache={len(ceps_cache_db)} / Novos={len(resultados)-len(ceps_cache_db)})"
        )

        self.writer.registrar_historico_geocoding(
            tenant_id, input_id, descricao, len(resultados)
        )

        return resultados

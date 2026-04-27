#sales_router/src/geocoding_engine/application/reprocess_invalids_service.py

import pandas as pd
import logging
import requests
import os

from geocoding_engine.domain.geo_validator import GeoValidator
from geocoding_engine.domain.address_normalizer import normalize_for_cache

logger = logging.getLogger(__name__)


# =========================================================
# 🔥 GOOGLE DIRETO
# =========================================================
def geocode_google_direto(endereco: str):

    api_key = os.getenv("GMAPS_API_KEY")

    if not api_key or not endereco:
        return None, None

    url = "https://maps.googleapis.com/maps/api/geocode/json"

    params = {
        "address": endereco,
        "key": api_key,
        "region": "br"
    }

    try:
        resp = requests.get(url, params=params, timeout=5)

        if resp.status_code != 200:
            return None, None

        data = resp.json()

        if data.get("status") == "OK":
            loc = data["results"][0]["geometry"]["location"]
            return loc["lat"], loc["lng"]

    except Exception as e:
        logger.warning(f"[GOOGLE_FAIL] {e}")

    return None, None


# =========================================================
# 🔥 SERVICE
# =========================================================
class ReprocessInvalidsService:

    def __init__(self, database_writer):
        self.writer = database_writer

    def _is_hard_fail(self, motivo: str) -> bool:
        return (
            "dado_faltante" in motivo or
            "duplicado" in motivo
        )

    def _dados_obrigatorios_ok(self, row):

        campos = [
            row.get("logradouro"),
            row.get("numero"),
            row.get("bairro"),
            row.get("cidade"),
            row.get("uf"),
        ]

        return all(
            c and str(c).strip().lower() not in ["", "nan", "none"]
            for c in campos
        )

    def _montar_endereco(self, row):

        numero = row.get("numero")

        # 🔥 CORREÇÃO CRÍTICA
        if pd.isna(numero):
            numero = ""

        elif isinstance(numero, float):
            numero = int(numero)

        else:
            numero = str(numero).strip()

            # remove .0 se vier como string
            if numero.endswith(".0"):
                numero = numero[:-2]

        return (
            f"{str(row.get('logradouro') or '').strip()} "
            f"{numero}, "
            f"{str(row.get('bairro') or '').strip()}, "
            f"{str(row.get('cidade') or '').strip()} - "
            f"{str(row.get('uf') or '').strip()}"
        ).replace(" ,", ",").strip()

    def _validar_geo_leve(self, lat, lon, cidade, uf) -> bool:
        status = GeoValidator.validar_ponto(lat, lon, cidade, uf)
        if status != "ok":
            logger.warning(
                f"[REPROCESS][VALIDACAO_UF_FAIL] status={status} "
                f"lat={lat} lon={lon} cidade={cidade} uf={uf}"
            )
            return False

        return True

    def execute(self, df_invalid: pd.DataFrame):

        if df_invalid is None or df_invalid.empty:
            return pd.DataFrame(), df_invalid

        df_invalid = df_invalid.copy()

        recuperados = []
        mantidos = []

        # métricas
        count_falha = 0
        count_fora = 0
        count_override = 0
        count_fallback = 0

        for _, row in df_invalid.iterrows():

            motivo = str(row.get("motivo_invalidacao") or "").lower()
            source = str(row.get("source") or "").lower()

            cidade = row.get("cidade")
            uf = row.get("uf")

            logger.info(
                f"[REPROCESS][START] motivo={motivo} | source={source} | cidade={cidade} | uf={uf}"
            )

            # =========================================================
            # 🔴 HARD FAIL
            # =========================================================
            if self._is_hard_fail(motivo):
                logger.warning("[REPROCESS][SKIP] hard_fail")
                mantidos.append(row)
                continue

            # =========================================================
            # 🔴 DADOS OBRIGATÓRIOS
            # =========================================================
            if not self._dados_obrigatorios_ok(row):
                row["motivo_invalidacao"] = "dado_faltante"

                logger.warning(f"[REPROCESS][DADO_FALTANTE] {row}")

                mantidos.append(row)
                continue

            endereco = self._montar_endereco(row)

            if not endereco or len(endereco) < 10:
                row["motivo_invalidacao"] = "endereco_incompleto"
                mantidos.append(row)
                continue

            endereco_norm = normalize_for_cache(endereco)

            # =========================================================
            # 🔥 CASO 1 — FALHA TOTAL
            # =========================================================
            if motivo == "falha":

                count_falha += 1

                logger.info(f"[REPROCESS][FALHA] fallback cidade → {cidade}-{uf}")

                lat, lon = geocode_google_direto(f"{cidade}, {uf}, Brasil")

                logger.info(f"[REPROCESS][FALHA][GOOGLE] lat={lat} lon={lon}")

                if lat is not None and lon is not None and self._validar_geo_leve(lat, lon, cidade, uf):
                    row["lat"] = lat
                    row["lon"] = lon
                    row["source"] = "fallback_cidade"
                    row["motivo_invalidacao"] = None

                    count_fallback += 1

                
                    recuperados.append(row)
                    continue

            # =========================================================
            # 🔥 FORA MUNICIPIO (REGRA CORRETA)
            # =========================================================
            if "fora_municipio" in motivo:

                count_fora += 1

                logger.info(
                    f"[REPROCESS][FORA_MUNICIPIO] source={source} → {endereco}"
                )

                # -----------------------------------------------------
                # CASO 1 — VEIO DO GOOGLE → fallback cidade
                # -----------------------------------------------------
                if source == "google":

                    logger.warning("[REPROCESS][GOOGLE_FORA] fallback cidade")

                    lat, lon = geocode_google_direto(f"{cidade}, {uf}, Brasil")

                    if lat is not None and lon is not None and self._validar_geo_leve(lat, lon, cidade, uf):
                        row["lat"] = lat
                        row["lon"] = lon
                        row["source"] = "fallback_cidade"
                        row["motivo_invalidacao"] = None

                        count_fallback += 1

                        recuperados.append(row)
                        continue

                    row["motivo_invalidacao"] = "fallback_falhou"
                    mantidos.append(row)
                    continue

                # -----------------------------------------------------
                # CASO 2 — CACHE OU NOMINATIM → TENTA GOOGLE
                # -----------------------------------------------------
                source_str = str(source or "").lower()

                if source_str == "cache" or source_str.startswith("nominatim"):

                    logger.info("[REPROCESS][TRY_GOOGLE]")

                    lat, lon = geocode_google_direto(endereco)

                    logger.info(f"[REPROCESS][GOOGLE_RESULT] lat={lat} lon={lon}")

                    # -------------------------------------------------
                    # 🔴 GOOGLE FALHOU
                    # -------------------------------------------------
                    if lat is None or lon is None:

                        logger.warning("[REPROCESS][GOOGLE_FAIL] → fallback cidade")

                        lat_fb, lon_fb = geocode_google_direto(f"{cidade}, {uf}, Brasil")

                        if lat_fb and lon_fb and self._validar_geo_leve(lat_fb, lon_fb, cidade, uf):
                            row["lat"] = lat_fb
                            row["lon"] = lon_fb
                            row["source"] = "fallback_cidade"
                            row["motivo_invalidacao"] = None

                            count_fallback += 1
                            recuperados.append(row)
                            continue

                        row["motivo_invalidacao"] = "falha_total"
                        mantidos.append(row)
                        continue

                    # -------------------------------------------------
                    # 🔥 VALIDAÇÃO UF (CORRIGIDA)
                    # -------------------------------------------------
                    if not self._validar_geo_leve(lat, lon, cidade, uf):

                        logger.warning("[REPROCESS][GEO_FAIL] → fallback cidade")

                        lat_fb, lon_fb = geocode_google_direto(f"{cidade}, {uf}, Brasil")

                        if lat_fb and lon_fb and self._validar_geo_leve(lat_fb, lon_fb, cidade, uf):
                            row["lat"] = lat_fb
                            row["lon"] = lon_fb
                            row["source"] = "fallback_cidade"
                            row["motivo_invalidacao"] = None

                            count_fallback += 1
                            recuperados.append(row)
                            continue

                        row["motivo_invalidacao"] = "falha_total"
                        mantidos.append(row)
                        continue
                    
                    # -------------------------------------------------
                    # 🟢 SUCESSO REAL (SEM VALIDAR POLÍGONO AQUI)
                    # -------------------------------------------------
                    logger.info("[REPROCESS][GOOGLE_OVERRIDE] SUCESSO")

                    row["lat"] = float(lat)
                    row["lon"] = float(lon)
                    row["source"] = "google_override"
                    row["motivo_invalidacao"] = None

                    count_override += 1
                    recuperados.append(row)

                    continue

            
            # =========================================================
            # 🔴 NÃO RECUPERADO
            # =========================================================
            logger.warning("[REPROCESS][FAIL_FINAL]")
            mantidos.append(row)

        df_recuperados = pd.DataFrame(recuperados)
        df_invalid_final = pd.DataFrame(mantidos)

        logger.info(
            f"[REPROCESS][SUMMARY] "
            f"falha={count_falha} | "
            f"fora_municipio={count_fora} | "
            f"override_google={count_override} | "
            f"fallback_cidade={count_fallback}"
        )

        logger.info(
            f"♻ Reprocessamento: recuperados={len(df_recuperados)} | "
            f"mantidos_invalidos={len(df_invalid_final)}"
        )

        return df_recuperados, df_invalid_final

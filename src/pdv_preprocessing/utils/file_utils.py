# sales_router/src/pdv_preprocessing/utils/file_utils.py

import os
import logging
import pandas as pd


INVALID_REASON_LABELS = {
    "campo_obrigatorio_cnpj": "CNPJ ausente ou invalido",
    "campo_obrigatorio_logradouro": "Logradouro obrigatorio ausente",
    "campo_obrigatorio_numero": "Numero obrigatorio ausente",
    "campo_obrigatorio_cidade": "Cidade obrigatoria ausente",
    "campo_obrigatorio_uf": "UF obrigatoria ausente",
    "numero_ausente_sem_cep": "Numero ausente e CEP nao informado",
    "cnpj_duplicado_arquivo": "CNPJ duplicado no arquivo",
    "cnpj_duplicado_input_id": "CNPJ duplicado neste processamento",
    "falha_input_geocoding": "Registro barrado antes do envio ao geocoding",
    "falha_integracao_geocoding": "Falha de integracao com o geocoding engine",
    "falha_geocoding": "Geocoding nao retornou coordenadas validas",
    "cidade_invalida": "Cidade ou UF nao encontrada na base de referencia",
    "fora_municipio": "Coordenada fora do municipio informado",
    "fallback_falhou": "Tentativa de recuperacao por fallback falhou",
}


def _friendly_invalid_reason(reason: str | None) -> str | None:
    if reason is None:
        return None

    raw_reason = str(reason).strip()
    if not raw_reason:
        return None

    if "|" not in raw_reason:
        return INVALID_REASON_LABELS.get(raw_reason, raw_reason)

    parts = [chunk.strip() for chunk in raw_reason.split("|") if chunk.strip()]
    labels = [INVALID_REASON_LABELS.get(part, part) for part in parts]
    return "; ".join(labels)


def enrich_invalidos_for_export(df_invalidos: pd.DataFrame) -> pd.DataFrame:
    if df_invalidos is None or df_invalidos.empty:
        return df_invalidos

    df_export = df_invalidos.copy()

    if "motivo_invalidade" in df_export.columns and "motivo_invalidade_label" not in df_export.columns:
        insert_at = df_export.columns.get_loc("motivo_invalidade") + 1
        df_export.insert(
            insert_at,
            "motivo_invalidade_label",
            df_export["motivo_invalidade"].apply(_friendly_invalid_reason),
        )

    return df_export


def detectar_separador(path: str) -> str:
    """Detecta automaticamente o separador do CSV."""
    with open(path, "r", encoding="utf-8-sig") as f:
        linha = f.readline()
        return ";" if ";" in linha else ","


def salvar_invalidos(df_invalidos: pd.DataFrame, pasta_base: str, input_id: str) -> str | None:
    try:
        if df_invalidos is None or df_invalidos.empty:
            return None

        df_export = enrich_invalidos_for_export(df_invalidos)

        pasta_invalidos = os.path.join(pasta_base, "invalidos")
        os.makedirs(pasta_invalidos, exist_ok=True)

        nome_arquivo = f"pdvs_invalidos_{input_id}.xlsx"
        caminho_saida = os.path.join(pasta_invalidos, nome_arquivo)

        df_export.to_excel(
            caminho_saida,
            index=False,
            engine="openpyxl"
        )

        logging.warning(f"⚠️ {len(df_invalidos)} inválidos salvos em: {caminho_saida}")
        return caminho_saida

    except Exception as e:
        logging.error(f"❌ Erro ao salvar inválidos: {e}", exc_info=True)
        return None


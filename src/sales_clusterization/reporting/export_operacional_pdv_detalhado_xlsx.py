# sales_router/src/sales_clusterization/reporting/export_operacional_pdv_detalhado_xlsx.py
#
# Exporta o XLSX detalhado da SETORIZAÇÃO da Execução Operacional:
# uma linha por PDV, colunas com setor, consultor e todos os detalhes do
# PDV. Gerado on-demand em memória (não persiste em disco).
#
# Difere do export da Simulação (export_cluster_pdv_detalhado_xlsx.py):
# lê do schema `operacional` e usa o consultor como centro do setor.

import io
import math

import pandas as pd
from loguru import logger

from database.db_connection import get_connection

# PDV é "isolado" quando a distância ao consultor passa deste múltiplo da
# distância média do setor (ilha visual). Adapta ao tamanho do setor.
ISOLADO_FATOR = 3.0

# coluna técnica -> cabeçalho amigável (define também a ORDEM no XLSX).
_COLUNAS = {
    "setor": "Setor",
    "consultor": "Consultor",
    "banda_status": "Status da banda",
    "cnpj": "CNPJ",
    "razao_social": "Razão social",
    "nome_fantasia": "Nome fantasia",
    "logradouro": "Logradouro",
    "numero": "Número",
    "bairro": "Bairro",
    "cidade": "Cidade",
    "uf": "UF",
    "cep": "CEP",
    "endereco": "Endereço completo",
    "lat": "Latitude",
    "lon": "Longitude",
    "status_geolocalizacao": "Status geocodificação",
    "pdv_vendas": "Vendas",
    "is_estrategico": "Estratégico",
    "tempo_atendimento_min": "Tempo de atendimento (min)",
    "dist_consultor_km": "Distância ao consultor (km)",
    "pdv_id": "PDV ID",
}

_QUERY_RUN = """
    SELECT id AS run_id
    FROM operacional.cluster_run
    WHERE tenant_id = %s AND clusterization_id = %s
    ORDER BY criado_em DESC
    LIMIT 1;
"""

_QUERY_PDVS = """
    SELECT
        cs.cluster_label             AS setor,
        cs.consultor_nome            AS consultor,
        cs.metrics->>'banda_status'  AS banda_status,
        cs.centro_lat                AS centro_lat,
        cs.centro_lon                AS centro_lon,
        csp.pdv_id                   AS pdv_id,
        COALESCE(p.cnpj, csp.cnpj)   AS cnpj,
        p.razao_social,
        p.nome_fantasia,
        p.logradouro,
        p.numero,
        COALESCE(p.bairro, csp.bairro)                       AS bairro,
        COALESCE(p.cidade, csp.cidade)                       AS cidade,
        COALESCE(p.uf, csp.uf)                               AS uf,
        p.cep,
        COALESCE(p.pdv_endereco_completo, csp.pdv_endereco_completo)
                                                             AS endereco,
        csp.lat                      AS lat,
        csp.lon                      AS lon,
        p.status_geolocalizacao,
        p.pdv_vendas,
        p.is_estrategico,
        p.tempo_atendimento_min
    FROM operacional.cluster_setor_pdv csp
    JOIN operacional.cluster_setor cs ON cs.id = csp.cluster_id
    LEFT JOIN operacional.pdvs p ON p.id = csp.pdv_id
    WHERE csp.tenant_id = %s AND csp.run_id = %s
    ORDER BY cs.cluster_label, csp.pdv_id;
"""


def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
        * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def exportar_operacional_pdv_detalhado(tenant_id, clusterization_id, output):
    logger.info(
        f"📋 Export setorização operacional | tenant={tenant_id} "
        f"| clusterization_id={clusterization_id}"
    )
    conn = get_connection()
    try:
        run_df = pd.read_sql_query(
            _QUERY_RUN, conn, params=(tenant_id, str(clusterization_id))
        )
        if run_df.empty:
            raise ValueError("Setorização não encontrada para este tenant.")
        run_id = int(run_df.iloc[0]["run_id"])

        df = pd.read_sql_query(_QUERY_PDVS, conn, params=(tenant_id, run_id))
    finally:
        conn.close()

    if df.empty:
        raise ValueError("Setorização sem PDVs para exportar.")

    # Distância de cada PDV ao consultor (centro do setor).
    def _dist(row):
        try:
            if (
                pd.isna(row["lat"]) or pd.isna(row["lon"])
                or pd.isna(row["centro_lat"]) or pd.isna(row["centro_lon"])
            ):
                return None
            return round(
                _haversine_km(
                    float(row["lat"]), float(row["lon"]),
                    float(row["centro_lat"]), float(row["centro_lon"]),
                ),
                3,
            )
        except (TypeError, ValueError):
            return None

    df["dist_consultor_km"] = df.apply(_dist, axis=1)

    # Vendas sem notação científica.
    df["pdv_vendas"] = pd.to_numeric(df["pdv_vendas"], errors="coerce").round(2)

    # PDVs isolados: distância ao consultor > ISOLADO_FATOR × média do setor.
    media_setor = (
        df.dropna(subset=["dist_consultor_km"])
        .groupby("setor")["dist_consultor_km"]
        .mean()
        .to_dict()
    )
    limiar = df["setor"].map(
        lambda s: (media_setor.get(s, 0) or 0) * ISOLADO_FATOR
    )
    df_isolados = (
        df[df["dist_consultor_km"].fillna(-1) > limiar]
        .sort_values(["setor", "dist_consultor_km"], ascending=[True, False])
    )
    logger.info(
        f"🏝️ PDVs isolados: {len(df_isolados)} de {len(df)} "
        f"(fator={ISOLADO_FATOR}× média do setor)"
    )

    # Estratégico legível.
    df["is_estrategico"] = df["is_estrategico"].map({True: "Sim", False: "Não"})

    # Seleciona/ordena/renomeia para os cabeçalhos amigáveis.
    cols = [c for c in _COLUNAS if c in df.columns]
    detalhe = df[cols].rename(columns=_COLUNAS)
    isolados = df_isolados[cols].rename(columns=_COLUNAS)

    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        detalhe.to_excel(writer, sheet_name="PDVs detalhado", index=False)
        if not isolados.empty:
            isolados.to_excel(writer, sheet_name="PDVs isolados", index=False)

    logger.success(
        f"✅ XLSX setorização operacional gerado | {len(detalhe)} PDVs"
    )


def operacional_pdv_detalhado_to_bytes(tenant_id, clusterization_id) -> bytes:
    """Gera o XLSX em memória e retorna os bytes (sem persistir em disco)."""
    buffer = io.BytesIO()
    exportar_operacional_pdv_detalhado(tenant_id, clusterization_id, buffer)
    buffer.seek(0)
    return buffer.getvalue()

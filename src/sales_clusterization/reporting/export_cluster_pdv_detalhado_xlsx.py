#sales_router/src/sales_clusterization/reporting/export_cluster_pdv_detalhado_xlsx.py

# ============================================================
# 📦 src/sales_clusterization/reporting/export_cluster_pdv_detalhado_xlsx.py
# ============================================================

import os
import math
import pandas as pd
import argparse
from loguru import logger
from database.db_connection import get_connection


# Múltiplo da distância média do setor acima do qual o PDV é
# classificado como "isolado" (ilha visual). Adapta ao tamanho
# do setor — setor grande tolera distâncias maiores em km.
ISOLADO_FATOR = 3.0


def _haversine_km(lat1, lon1, lat2, lon2):
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    )
    return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def exportar_cluster_pdv_detalhado(tenant_id: int, clusterization_id: str):
    logger.info(f"📋 Exportando PDVs detalhados | tenant={tenant_id} | clusterization_id={clusterization_id}")

    conn = get_connection()

    # 🔍 Busca o run_id mais recente vinculado à clusterization_id
    query_run = f"""
        SELECT id AS run_id
        FROM cluster_run
        WHERE tenant_id = {tenant_id} AND clusterization_id = '{clusterization_id}'
        ORDER BY criado_em DESC
        LIMIT 1;
    """
    run_df = pd.read_sql_query(query_run, conn)
    if run_df.empty:
        conn.close()
        raise ValueError(f"❌ Nenhum run encontrado para clusterization_id={clusterization_id}")

    run_id = int(run_df.iloc[0]['run_id'])
    logger.info(f"🔎 Run identificado: {run_id}")

    # 📋 Detalhe PDV (view existente)
    query = f"""
        SELECT *
        FROM v_cluster_pdv_detalhado
        WHERE tenant_id = {tenant_id} AND run_id = {run_id}
        ORDER BY cluster_label, pdv_id;
    """
    df = pd.read_sql_query(query, conn)

    # 📍 Centros dos setores — pra medir distância de cada PDV ao centro
    query_setores = f"""
        SELECT id AS cluster_id, cluster_label, centro_lat, centro_lon
        FROM cluster_setor
        WHERE run_id = {run_id};
    """
    df_setores = pd.read_sql_query(query_setores, conn)
    conn.close()

    if df.empty:
        raise ValueError(f"❌ Nenhum dado encontrado em v_cluster_pdv_detalhado para run_id={run_id}")

    # =========================================
    # 🔧 Correção: evitar notação científica
    # =========================================
    if "pdv_vendas" in df.columns:
        df["pdv_vendas"] = pd.to_numeric(df["pdv_vendas"], errors="coerce").round(2)

    # =========================================
    # 🏝️ Detecção de PDVs isolados (ilhas visuais)
    # ----------------------------------------
    # Para cada PDV, calcula dist haversine até o centro do seu setor.
    # Marca como isolado se distância > ISOLADO_FATOR × média do setor.
    # Adapta automaticamente: setor pequeno = limiar pequeno; setor grande
    # = limiar grande. Evita falso-positivo em setores naturalmente extensos.
    # =========================================
    df_isolados = pd.DataFrame()
    if not df_setores.empty:
        centros = {
            int(row["cluster_id"]): (float(row["centro_lat"]), float(row["centro_lon"]))
            for _, row in df_setores.iterrows()
        }
        dists = []
        for _, row in df.iterrows():
            cid = int(row["cluster_id"])
            centro = centros.get(cid)
            if centro is None or pd.isna(row["lat"]) or pd.isna(row["lon"]):
                dists.append(None)
                continue
            dists.append(_haversine_km(row["lat"], row["lon"], centro[0], centro[1]))
        df["dist_centro_km"] = dists

        # Limiar por setor (média × fator)
        media_por_setor = (
            df.dropna(subset=["dist_centro_km"])
            .groupby("cluster_id")["dist_centro_km"]
            .mean()
            .to_dict()
        )
        df["limiar_isolado_km"] = df["cluster_id"].map(
            lambda c: round((media_por_setor.get(int(c), 0) or 0) * ISOLADO_FATOR, 3)
        )
        df["isolado"] = (
            df["dist_centro_km"].fillna(-1) > df["limiar_isolado_km"]
        )

        df_isolados = (
            df[df["isolado"]]
            .copy()
            .sort_values(["cluster_label", "dist_centro_km"], ascending=[True, False])
        )

        logger.info(
            f"🏝️ PDVs isolados detectados: {len(df_isolados)} de {len(df)} "
            f"(fator={ISOLADO_FATOR}× média de cada setor)"
        )

    output_dir = f"output/reports/{tenant_id}"
    os.makedirs(output_dir, exist_ok=True)
    output_path = os.path.join(
        output_dir, f"cluster_pdv_detalhado_{clusterization_id}.xlsx"
    )

    # Duas sheets: detalhe completo + alerta de isolados
    with pd.ExcelWriter(output_path, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="PDVs detalhado", index=False)
        if not df_isolados.empty:
            df_isolados.to_excel(writer, sheet_name="PDVs isolados", index=False)

    logger.success(f"✅ Arquivo salvo em: {output_path}")



if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exporta PDVs detalhados por cluster (SalesRouter)")
    parser.add_argument("--tenant_id", type=int, required=True, help="ID do tenant")
    parser.add_argument("--clusterization_id", type=str, required=True, help="UUID da clusterização")
    args = parser.parse_args()

    exportar_cluster_pdv_detalhado(args.tenant_id, args.clusterization_id)

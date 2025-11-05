# ============================================================
# 📦 src/sales_routing/reporting/export_pdvs_por_cluster.py
# ============================================================

import os
import argparse
import pandas as pd
from loguru import logger
from pathlib import Path
from src.database.db_connection import get_connection


def exportar_pdvs_por_cluster(tenant_id: int, routing_id: str):
    """
    Exporta todos os PDVs (com informações completas + cluster + subcluster)
    para um CSV filtrado por tenant_id e routing_id.
    """
    logger.info(f"📤 Exportando PDVs por cluster | tenant={tenant_id} | routing_id={routing_id}")

    sql = """
        SELECT
            pdv_id,
            tenant_id,
            input_id,
            cnpj,
            logradouro,
            numero,
            bairro,
            cidade,
            uf,
            cep,
            pdv_endereco_completo,
            pdv_lat,
            pdv_lon,
            status_geolocalizacao,
            pdv_vendas,
            input_descricao,
            cluster_run_id,
            clusterization_id,
            cluster_id,
            cluster_nome,
            cluster_centro_lat,
            cluster_centro_lon,
            subcluster_run_id,
            routing_id,
            subcluster_numero,
            ordem_rota,
            subcluster_criado_em
        FROM vw_pdv_cluster_subcluster
        WHERE tenant_id = %s
          AND routing_id = %s
        ORDER BY cluster_id, subcluster_numero, ordem_rota;
    """

    try:
        conn = get_connection()
        df = pd.read_sql(sql, conn, params=(tenant_id, routing_id))
        conn.close()
        logger.debug(f"🔍 Registros retornados: {len(df)}")
    except Exception as e:
        logger.error(f"❌ Erro ao consultar banco: {e}")
        return

    if df.empty:
        logger.warning("⚠️ Nenhum registro encontrado para os filtros informados.")
        return

    # ================================
    # 💾 Caminho de saída
    # ================================
    output_dir = Path(f"output/reports/{tenant_id}")
    try:
        output_dir.mkdir(parents=True, exist_ok=True)
        os.chmod(output_dir, 0o777)
    except Exception as e:
        logger.warning(f"⚠️ Falha ao ajustar permissões do diretório: {e}")

    arquivo_csv = output_dir / f"pdvs_por_cluster_{routing_id}.csv"

    try:
        df.to_csv(arquivo_csv, index=False, sep=";", encoding="utf-8-sig")
        logger.success(f"✅ Arquivo exportado com sucesso: {arquivo_csv}")
    except Exception as e:
        logger.error(f"❌ Erro ao salvar arquivo CSV: {e}")


# ============================================================
# 🚀 Execução via CLI
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exporta PDVs com cluster e subcluster.")
    parser.add_argument("--tenant", type=int, required=True, help="ID do tenant")
    parser.add_argument("--routing_id", type=str, required=True, help="Routing ID da execução")
    args = parser.parse_args()

    exportar_pdvs_por_cluster(args.tenant, args.routing_id)

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
    para um arquivo Excel (.xlsx) filtrado por tenant_id e routing_id.
    Corrige CNPJ com máscara e formata coordenadas corretamente.
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

    # =======================================================
    # 🔍 Consulta ao banco
    # =======================================================
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

    # =======================================================
    # 🧹 Normalização de campos
    # =======================================================
    def normalizar_coord(valor):
        """Corrige coordenadas multiplicadas por 10⁶ ou com vírgula decimal."""
        try:
            v = float(str(valor).replace(",", "."))
            if abs(v) > 90:
                v = v / 1e6
            return round(v, 6)
        except:
            return None

    # ➕ CNPJ formatado com máscara
    if "cnpj" in df.columns:
        df["cnpj"] = (
            df["cnpj"]
            .astype(str)
            .str.replace(r"\.0$", "", regex=True)
            .str.replace(r"[^0-9]", "", regex=True)
            .str.zfill(14)
            .apply(lambda x: f"{x[:2]}.{x[2:5]}.{x[5:8]}/{x[8:12]}-{x[12:]}")
        )

    # ➕ Corrige coordenadas
    for col in ["pdv_lat", "pdv_lon", "cluster_centro_lat", "cluster_centro_lon"]:
        if col in df.columns:
            df[col] = df[col].apply(normalizar_coord)

    # =======================================================
    # 💾 Caminho de saída
    # =======================================================
    output_dir = Path(f"output/reports/{tenant_id}")
    output_dir.mkdir(parents=True, exist_ok=True)
    try:
        os.chmod(output_dir, 0o777)
    except Exception as e:
        logger.warning(f"⚠️ Falha ao ajustar permissões: {e}")

    arquivo_xlsx = output_dir / f"pdvs_por_cluster_{routing_id}.xlsx"

    # =======================================================
    # 📊 Exportação Excel
    # =======================================================
    try:
        with pd.ExcelWriter(arquivo_xlsx, engine="openpyxl") as writer:
            df.to_excel(writer, index=False, sheet_name="PDVs_Clusters")

            # Ajuste automático de largura de coluna
            ws = writer.sheets["PDVs_Clusters"]
            for col_cells in ws.columns:
                max_length = 0
                col_letter = col_cells[0].column_letter
                for cell in col_cells:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                ws.column_dimensions[col_letter].width = min(max_length + 2, 60)

        logger.success(f"✅ Arquivo Excel exportado com sucesso: {arquivo_xlsx}")
        return arquivo_xlsx

    except Exception as e:
        logger.error(f"❌ Erro ao salvar XLSX: {e}")
        return None


# ============================================================
# 🚀 Execução via CLI
# ============================================================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Exporta PDVs com cluster e subcluster (Excel).")
    parser.add_argument("--tenant", type=int, required=True, help="ID do tenant")
    parser.add_argument("--routing_id", type=str, required=True, help="Routing ID da execução")
    args = parser.parse_args()

    exportar_pdvs_por_cluster(args.tenant, args.routing_id)

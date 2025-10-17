# ============================================================
# 📦 src/sales_routing/reporting/vendedores_summary_service.py
# ============================================================

import os
import json
import pandas as pd
from loguru import logger
from src.database.db_connection import get_connection_context


class VendedoresSummaryService:
    """
    Gera relatório consolidado de vendedores:
      - 1 linha por vendedor
      - Usa dados diretos de sales_subcluster (sem JOIN com PDVs)
      - Inclui totais e médias diárias/por rota
      - Exporta CSV (pt-BR) e JSON
    """

    def __init__(self, tenant_id: int):
        self.tenant_id = tenant_id
        self.pasta_output = os.path.join("output", "reports", str(tenant_id))
        os.makedirs(self.pasta_output, exist_ok=True)

    # =========================================================
    # 1️⃣ Carrega dados consolidados do banco
    # =========================================================
    def carregar_dados(self):
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    SELECT
                        vb.vendedor_id,
                        vb.base_cidade,
                        vb.base_bairro,
                        vb.total_pdvs,
                        COUNT(s.id) AS total_rotas,
                        SUM(s.dist_total_km) AS km_total,
                        SUM(s.tempo_total_min) AS tempo_total_min
                    FROM sales_vendedor_base vb
                    LEFT JOIN sales_subcluster s
                        ON s.vendedor_id = vb.vendedor_id
                       AND s.tenant_id = vb.tenant_id
                    WHERE vb.tenant_id = %s
                    GROUP BY vb.vendedor_id, vb.base_cidade, vb.base_bairro, vb.total_pdvs
                    ORDER BY vb.vendedor_id;
                """, (self.tenant_id,))
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            logger.warning("⚠️ Nenhum vendedor encontrado para gerar o resumo.")
            return None

        # =====================================================
        # 🔹 Cálculos adicionais (médias diárias e por rota)
        # =====================================================
        df["km_medio_por_rota"] = (df["km_total"] / df["total_rotas"]).round(1)
        df["tempo_medio_por_rota"] = (df["tempo_total_min"] / df["total_rotas"]).round(1)
        df["km_medio_diario"] = (df["km_total"] / 20).round(1)
        df["tempo_medio_diario"] = (df["tempo_total_min"] / 20).round(1)

        # =====================================================
        # 🔹 Formatação numérica para padrão brasileiro
        # =====================================================
        colunas_numericas = [
            "total_pdvs", "total_rotas",
            "km_total", "km_medio_por_rota", "km_medio_diario",
            "tempo_total_min", "tempo_medio_por_rota", "tempo_medio_diario"
        ]
        for col in colunas_numericas:
            df[col] = df[col].apply(
                lambda x: f"{x:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".") if pd.notna(x) else ""
            )

        logger.info(f"📊 {len(df)} vendedores consolidados e formatados para tenant={self.tenant_id}")
        return df

    # =========================================================
    # 2️⃣ Exporta CSV e JSON
    # =========================================================
    def exportar(self, df: pd.DataFrame):
        if df is None or df.empty:
            logger.warning("⚠️ Nenhum dado para exportar.")
            return None, None

        csv_path = os.path.join(self.pasta_output, "sales_vendedores_summary.csv")
        json_path = os.path.join(self.pasta_output, "sales_vendedores_summary.json")

        # CSV formatado para Excel (pt-BR)
        df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

        # JSON puro (mantém valores numéricos originais)
        df_json = df.to_dict(orient="records")
        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(df_json, f, ensure_ascii=False, indent=4)

        logger.success(f"💾 Relatório de vendedores salvo em:\n📁 {csv_path}\n📁 {json_path}")
        return csv_path, json_path

    # =========================================================
    # 3️⃣ Pipeline completo
    # =========================================================
    def gerar_relatorio(self):
        df = self.carregar_dados()
        if df is not None:
            return self.exportar(df)
        return None, None

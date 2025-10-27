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
    Gera relatório consolidado de vendedores para um assign_id específico:
      - 1 linha por vendedor
      - Usa dados de sales_vendedor_base, sales_subcluster_vendedor, sales_pdv_vendedor e pdvs
      - Inclui totais e médias diárias/por rota e total/média de vendas
    """

    def __init__(self, tenant_id: int, assign_id: str):
        self.tenant_id = tenant_id
        self.assign_id = assign_id
        self.pasta_output = os.path.join("output", "reports", str(tenant_id))
        os.makedirs(self.pasta_output, exist_ok=True)

        # =========================================================
    # 1️⃣ Carrega dados consolidados do banco (com CTE e deduplicação)
    # =========================================================
    def carregar_dados(self):
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    WITH rotas_agrupadas AS (
                        SELECT
                            sv.tenant_id,
                            sv.assign_id,
                            sv.vendedor_id,
                            COUNT(DISTINCT s.id) AS total_rotas,
                            ROUND(SUM(DISTINCT s.dist_total_km)::numeric, 2) AS km_total,
                            ROUND(SUM(DISTINCT s.tempo_total_min)::numeric, 2) AS tempo_total_min
                        FROM sales_subcluster_vendedor sv
                        JOIN sales_subcluster s
                            ON s.tenant_id = sv.tenant_id
                           AND s.cluster_id = sv.cluster_id
                           AND s.subcluster_seq = sv.subcluster_seq
                           AND s.assign_id = sv.assign_id
                        WHERE sv.tenant_id = %s
                          AND sv.assign_id = %s
                        GROUP BY sv.tenant_id, sv.assign_id, sv.vendedor_id
                    )
                    SELECT
                        vb.vendedor_id,
                        vb.base_cidade,
                        vb.base_bairro,
                        vb.total_pdvs,
                        r.total_rotas,
                        r.km_total,
                        r.tempo_total_min,
                        ROUND(SUM(p.pdv_vendas)::numeric, 2) AS vendas_total,
                        ROUND(AVG(p.pdv_vendas)::numeric, 2) AS vendas_media
                    FROM sales_vendedor_base vb
                    LEFT JOIN rotas_agrupadas r
                        ON r.tenant_id = vb.tenant_id
                       AND r.assign_id = vb.assign_id
                       AND r.vendedor_id = vb.vendedor_id
                    LEFT JOIN sales_pdv_vendedor pv
                        ON pv.tenant_id = vb.tenant_id
                       AND pv.assign_id = vb.assign_id
                       AND pv.vendedor_id = vb.vendedor_id
                    LEFT JOIN pdvs p
                        ON p.id = pv.pdv_id
                    WHERE vb.tenant_id = %s
                      AND vb.assign_id = %s
                    GROUP BY vb.vendedor_id, vb.base_cidade, vb.base_bairro,
                             vb.total_pdvs, r.total_rotas, r.km_total, r.tempo_total_min
                    ORDER BY vb.vendedor_id;
                """, (self.tenant_id, self.assign_id, self.tenant_id, self.assign_id))

                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)

        if df.empty:
            logger.warning(f"⚠️ Nenhum vendedor encontrado para tenant={self.tenant_id} e assign_id={self.assign_id}.")
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
            "tempo_total_min", "tempo_medio_por_rota", "tempo_medio_diario",
            "vendas_total", "vendas_media"
        ]
        for col in colunas_numericas:
            df[col] = df[col].apply(
                lambda x: f"{x:,.1f}".replace(",", "X").replace(".", ",").replace("X", ".")
                if pd.notna(x) else ""
            )

        logger.info(f"📊 {len(df)} vendedores consolidados (tenant={self.tenant_id}, assign_id={self.assign_id})")
        return df



    # =========================================================
    # 2️⃣ Exporta CSV e JSON
    # =========================================================
    def exportar(self, df: pd.DataFrame):
        if df is None or df.empty:
            logger.warning("⚠️ Nenhum dado para exportar.")
            return None, None

        csv_path = os.path.join(
            self.pasta_output, f"sales_vendedores_summary_{self.assign_id}.csv"
        )
        json_path = os.path.join(
            self.pasta_output, f"sales_vendedores_summary_{self.assign_id}.json"
        )

        df.to_csv(csv_path, index=False, sep=";", encoding="utf-8-sig")

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


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Gera resumo consolidado de vendedores por assign_id")
    parser.add_argument("--tenant", type=int, required=True, help="Tenant ID")
    parser.add_argument("--assign_id", type=str, required=True, help="Assign ID (UUID da atribuição)")
    args = parser.parse_args()

    VendedoresSummaryService(tenant_id=args.tenant, assign_id=args.assign_id).gerar_relatorio()

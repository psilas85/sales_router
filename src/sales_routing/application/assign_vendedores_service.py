# src/sales_routing/application/assign_vendedores_service.py

# ============================================================
# 📦 src/sales_routing/application/assign_vendedores_service.py
# ============================================================

import math
import pandas as pd
import uuid
from loguru import logger
from psycopg2.extras import execute_values
from src.database.db_connection import get_connection_context
from src.sales_routing.infrastructure.database_reader import SalesRoutingDatabaseReader
from src.sales_routing.infrastructure.database_writer import SalesRoutingDatabaseWriter


class AssignVendedoresService:
    """
    Serviço de atribuição espacial de vendedores às rotas já salvas (imutável).
    - Lê rotas operacionais do banco (sales_subcluster)
    - Cria assign_id e registra histórico
    - Persiste vínculos em sales_subcluster_vendedor (sem sobrescrever)
    - Calcula base geométrica ponderada por PDVs das rotas
    """

    def __init__(
        self,
        tenant_id: int,
        routing_id: str,
        assign_id: str,
        descricao: str,
        usuario: str,
        freq_mensal: int = 1,
        dias_uteis: int = 20,
        workday_min: int = 500,
    ):
        self.tenant_id = tenant_id
        self.routing_id = routing_id
        self.assign_id = assign_id
        self.descricao = descricao
        self.usuario = usuario
        self.freq_mensal = freq_mensal
        self.dias_uteis = dias_uteis
        self.workday_min = workday_min
        self.db_reader = SalesRoutingDatabaseReader()
        self.db_writer = SalesRoutingDatabaseWriter()

    # =========================================================
    # 1️⃣ Calcula capacidade teórica por vendedor (em rotas)
    # =========================================================
    def _calcular_parametros(self, total_rotas: int):
        """Define quantas rotas cada vendedor pode atender e quantos são necessários."""
        rotas_por_vendedor = max(1, round(self.dias_uteis / self.freq_mensal))
        vendedores_necessarios = math.ceil(total_rotas / rotas_por_vendedor)
        vendedores_necessarios = min(vendedores_necessarios, total_rotas)

        logger.info(f"🧮 Freq. mensal: {self.freq_mensal}x | Dias úteis: {self.dias_uteis}")
        logger.info(f"👥 Cada vendedor pode atender até {rotas_por_vendedor} rotas/mês.")
        logger.info(f"📊 Total de rotas: {total_rotas} → Necessários {vendedores_necessarios} vendedores.")
        return rotas_por_vendedor, vendedores_necessarios

    # =========================================================
    # 2️⃣ Atribuição geográfica (sweep por rotas)
    # =========================================================
    def _atribuir_por_sweep(self, rotas_df: pd.DataFrame):
        """
        Atribui rotas por varredura geográfica (sweep):
        - Mantém agrupamento por rotas (não PDVs)
        - Ordena Oeste→Leste (lon) e Sul→Norte (lat)
        - Cada vendedor recebe um conjunto de rotas (capacidade teórica mensal)
        """
        import numpy as np

        rotas_por_vendedor = max(1, round(self.dias_uteis / self.freq_mensal))
        logger.info(
            f"📦 Agrupando rotas: {rotas_por_vendedor} por vendedor "
            f"(freq={self.freq_mensal}x/mês, dias_úteis={self.dias_uteis})"
        )

        rotas_df = rotas_df.sort_values(by=["centro_lon", "centro_lat"]).reset_index(drop=True)
        vendedor_id = 1
        rotas_df["vendedor_id"] = np.nan
        contador = 0

        for idx in range(len(rotas_df)):
            rotas_df.at[idx, "vendedor_id"] = vendedor_id
            contador += 1
            if contador >= rotas_por_vendedor:
                vendedor_id += 1
                contador = 0

        rotas_df["vendedor_id"] = rotas_df["vendedor_id"].astype(int)
        logger.success(f"✅ Atribuição concluída: {vendedor_id} vendedores (por rotas).")
        return rotas_df, vendedor_id

    # =========================================================
    # 3️⃣ Registro histórico
    # =========================================================
    def _registrar_historico(self):
        """Cria entrada no histórico de atribuições."""
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO historico_assign_jobs
                    (tenant_id, assign_id, routing_id, descricao, criado_por, criado_em)
                    VALUES (%s, %s, %s, %s, %s, NOW());
                    """,
                    (self.tenant_id, self.assign_id, self.routing_id, self.descricao, self.usuario),
                )
                conn.commit()
        logger.info(f"🧾 Histórico salvo | assign_id={self.assign_id}")

    # =========================================================
    # 4️⃣ Persistência dos vínculos (sem sobrescrever assign_id antigos)
    # =========================================================
    def _salvar_vinculos(self, rotas_df: pd.DataFrame):
        """Insere os vínculos subcluster → vendedor sem sobrescrever atribuições anteriores."""
        registros = [
            (
                self.tenant_id,
                self.assign_id,
                self.routing_id,
                int(r["cluster_id"]),
                int(r["subcluster_seq"]),
                int(r["vendedor_id"]),
            )
            for _, r in rotas_df.iterrows()
        ]

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                # 🔹 1. Insere novos vínculos (sem duplicar)
                execute_values(
                    cur,
                    """
                    INSERT INTO sales_subcluster_vendedor
                    (tenant_id, assign_id, routing_id, cluster_id, subcluster_seq, vendedor_id)
                    VALUES %s
                    ON CONFLICT DO NOTHING;
                    """,
                    registros,
                )

                # 🔹 2. Atualiza sales_subcluster APENAS quando ainda não há assign_id definido
                cur.execute(
                    """
                    UPDATE sales_subcluster s
                    SET assign_id = v.assign_id,
                        vendedor_id = v.vendedor_id
                    FROM sales_subcluster_vendedor v
                    WHERE s.tenant_id = v.tenant_id
                    AND s.cluster_id = v.cluster_id
                    AND s.subcluster_seq = v.subcluster_seq
                    AND s.tenant_id = %s
                    AND v.assign_id = %s
                    AND s.assign_id IS NULL;
                    """,
                    (self.tenant_id, self.assign_id),
                )


                conn.commit()

        logger.success(
            f"💾 {len(registros)} vínculos gravados em sales_subcluster_vendedor "
            f"e sincronizados em sales_subcluster (sem sobrescrever assign_id anteriores)."
        )

        # =========================================================
    # 4️⃣b Persistência direta PDV → Vendedor (sales_pdv_vendedor)
    # =========================================================
    def _salvar_pdv_vinculos(self):
        """
        Gera e salva vínculos diretos entre PDVs e vendedores,
        eliminando a dependência de joins em múltiplas tabelas.
        """
        logger.info("🔄 Gerando vínculos diretos PDV → vendedor (sales_pdv_vendedor)...")

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO sales_pdv_vendedor (
                        tenant_id,
                        assign_id,
                        routing_id,
                        vendedor_id,
                        pdv_id,
                        cluster_id,
                        subcluster_seq,
                        cidade,
                        uf
                    )
                    SELECT
                        v.tenant_id,
                        v.assign_id,
                        v.routing_id,
                        v.vendedor_id,
                        sp.pdv_id,
                        s.cluster_id,
                        s.subcluster_seq,
                        p.cidade,
                        p.uf
                    FROM sales_subcluster_vendedor v
                    JOIN sales_subcluster_pdv sp
                      ON v.tenant_id = sp.tenant_id
                     AND v.cluster_id = sp.cluster_id
                     AND v.subcluster_seq = sp.subcluster_seq
                    JOIN pdvs p
                      ON p.id = sp.pdv_id
                    JOIN sales_subcluster s
                      ON s.tenant_id = v.tenant_id
                     AND s.cluster_id = v.cluster_id
                     AND s.subcluster_seq = v.subcluster_seq
                    WHERE v.tenant_id = %s
                      AND v.assign_id = %s
                    ON CONFLICT (tenant_id, assign_id, pdv_id) DO NOTHING;
                    """,
                    (self.tenant_id, self.assign_id),
                )
                conn.commit()

        logger.success(f"💾 Vínculos PDV → vendedor salvos com sucesso (assign_id={self.assign_id}).")


        # =========================================================
    # 5️⃣ Cálculo de base dos vendedores (com cidade/bairro)
    # =========================================================
    def calcular_bases_vendedores(self, rotas_df: pd.DataFrame):
        """
        Calcula base (centroide ponderado) de cada vendedor com assign_id.
        Inclui cidade e bairro da base a partir dos PDVs associados ao vendedor.
        """
        resultados = []

        with get_connection_context() as conn:
            with conn.cursor() as cur:
                for vendedor_id, grupo in rotas_df.groupby("vendedor_id"):
                    total_pdvs = grupo["n_pdvs"].sum() if "n_pdvs" in grupo.columns else len(grupo)

                    if total_pdvs == 0:
                        logger.warning(f"⚠️ Vendedor {vendedor_id} sem PDVs — usando média simples.")
                        base_lat = grupo["centro_lat"].mean()
                        base_lon = grupo["centro_lon"].mean()
                    else:
                        base_lat = (grupo["centro_lat"] * grupo["n_pdvs"]).sum() / total_pdvs
                        base_lon = (grupo["centro_lon"] * grupo["n_pdvs"]).sum() / total_pdvs

                    # 🔹 Seleciona cidade e bairro do PDV mais próximo à base calculada
                    cur.execute(
                        """
                        SELECT p.cidade, p.bairro
                        FROM sales_pdv_vendedor pv
                        JOIN pdvs p ON p.id = pv.pdv_id
                        WHERE pv.tenant_id = %s
                          AND pv.assign_id = %s
                          AND pv.vendedor_id = %s
                          AND p.pdv_lat IS NOT NULL
                          AND p.pdv_lon IS NOT NULL
                        ORDER BY ((p.pdv_lat - %s)^2 + (p.pdv_lon - %s)^2)
                        LIMIT 1;
                        """,
                        (self.tenant_id, self.assign_id, vendedor_id, base_lat, base_lon),
                    )

                    row = cur.fetchone()
                    base_cidade, base_bairro = (row if row else (None, None))

                    resultados.append(
                        {
                            "vendedor_id": vendedor_id,
                            "base_cidade": base_cidade,
                            "base_bairro": base_bairro,
                            "base_lat": base_lat,
                            "base_lon": base_lon,
                            "total_rotas": len(grupo),
                            "total_pdvs": int(total_pdvs),
                        }
                    )

                # 🔄 Limpa registros antigos apenas do mesmo tenant e assign
                cur.execute(
                    "DELETE FROM sales_vendedor_base WHERE tenant_id = %s AND assign_id = %s;",
                    (self.tenant_id, self.assign_id),
                )

                # 💾 Insere resultados completos
                for r in resultados:
                    cur.execute(
                        """
                        INSERT INTO sales_vendedor_base
                        (tenant_id, assign_id, vendedor_id, base_cidade, base_bairro,
                         base_lat, base_lon, total_rotas, total_pdvs, data_calculo)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,NOW());
                        """,
                        (
                            self.tenant_id,
                            self.assign_id,
                            r["vendedor_id"],
                            r["base_cidade"],
                            r["base_bairro"],
                            r["base_lat"],
                            r["base_lon"],
                            r["total_rotas"],
                            r["total_pdvs"],
                        ),
                    )
                conn.commit()

        logger.success(
            f"🏠 {len(resultados)} bases de vendedores calculadas (com cidade/bairro) e salvas "
            f"(assign_id={self.assign_id})."
        )

    # =========================================================
    # 6️⃣ Execução principal
    # =========================================================
    def executar(self, uf: str = None, cidade: str = None):
        """Executa atribuição completa sem sobrescrita."""
        from src.sales_routing.reporting.vendedores_summary_service import VendedoresSummaryService

        logger.info(f"🏁 Iniciando atribuição | tenant={self.tenant_id} | routing_id={self.routing_id}")
        logger.info(f"📍 Filtrando rotas por routing_id={self.routing_id}")


        # 1️⃣ Registra histórico
        self._registrar_historico()

        # 2️⃣ Carrega rotas
        rotas = self.db_reader.get_operational_routes(self.tenant_id, self.routing_id, uf=uf, cidade=cidade)

        if not rotas:
            logger.warning("❌ Nenhuma rota encontrada para este tenant.")
            return

        rotas_df = pd.DataFrame(rotas).dropna(subset=["centro_lat", "centro_lon"])
        total_rotas = len(rotas_df)
        logger.info(f"📦 {total_rotas} rotas carregadas para atribuição.")

        # 3️⃣ Atribui vendedores
        rotas_df, vendedores_necessarios = self._atribuir_por_sweep(rotas_df)

            # 4️⃣ Persiste vínculos
        self._salvar_vinculos(rotas_df)

        # 4️⃣b Persiste vínculos diretos PDV → vendedor
        self._salvar_pdv_vinculos()


        # 5️⃣ Calcula bases e gera relatório
        self.calcular_bases_vendedores(rotas_df)
        VendedoresSummaryService(self.tenant_id, self.assign_id).gerar_relatorio()

        logger.success(
            f"✅ Atribuição finalizada | {vendedores_necessarios} vendedores | "
            f"assign_id={self.assign_id} | routing_id={self.routing_id}"
        )

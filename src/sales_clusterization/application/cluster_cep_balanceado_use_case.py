#sales_router/src/sales_clusterization/application/cluster_cep_balanceado_use_case.py

# ============================================================
# 📦 src/sales_clusterization/application/cluster_cep_balanceado_use_case.py
# ============================================================

import pandas as pd
import numpy as np
import time
import uuid
from loguru import logger
from database.db_connection import get_connection
from sales_clusterization.application.cluster_cep_ativa_use_case import ClusterCEPAtivaUseCase
from sales_clusterization.domain.haversine_utils import haversine


class ClusterCEPBalanceadoUseCase(ClusterCEPAtivaUseCase):

    def __init__(
        self,
        *args,
        min_ceps: int = None,
        max_ceps: int = None,
        max_merge_km: float = 3.0,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.min_ceps = min_ceps
        self.max_ceps = max_ceps
        self.max_merge_km = max_merge_km
        self.max_iter = 10

    # ============================================================
    # ▶️ Execução principal
    # ============================================================
    def execute(self):
        inicio_execucao = time.time()
        logger.info("🚀 Iniciando clusterização balanceada de CEPs...")

        clusterization_id = str(uuid.uuid4())
        resultado_inicial = super().execute()

        if not resultado_inicial:
            logger.error("❌ Falha na clusterização base.")
            return None

        clusterization_id_base = resultado_inicial["clusterization_id"]

        # ============================================================
        # 1) LEITURA ROBUSTA DOS CEPS — FIX ABSOLUTO DO PROBLEMA
        # ============================================================
        df_ceps = self._read_df_sql(
            "SELECT * FROM mkp_cluster_cep WHERE clusterization_id = %s;",
            (clusterization_id_base,)
        )

        if df_ceps.empty:
            logger.error("❌ Nenhum CEP encontrado para balanceamento.")
            return None

        # ============================================================
        # 2) LEITURA DOS CENTROS REAIS
        # ============================================================
        df_centros = self._read_df_sql(
            """
            SELECT DISTINCT 
                cluster_id,
                cluster_lat AS lat,
                cluster_lon AS lon,
                centro_nome,
                centro_cnpj,
                cluster_bairro
            FROM mkp_cluster_cep
            WHERE clusterization_id = %s
            """,
            (clusterization_id_base,)
        )

        # ============================================================
        # 🧹 FIX — DEDUPE DE CEP (impede erro ON CONFLICT)
        # ============================================================
        df_ceps["cep"] = df_ceps["cep"].astype(str).str.replace(r"\D+", "", regex=True)
        df_ceps = df_ceps[df_ceps["cep"].str.len() == 8]
        df_ceps = df_ceps.drop_duplicates(subset=["cep"]).copy()

        df_bal = self.balancear_clusters(df_ceps.copy(), df_centros.copy())
        df_bal["modo_clusterizacao"] = "balanceada"
        df_bal["clusterization_id"] = clusterization_id

        # ============================================================
        # 💾 GRAVAÇÃO
        # ============================================================
        logger.info("💾 Gravando clusters balanceados...")
        self.writer.inserir_mkp_cluster_cep(df_bal.to_dict(orient="records"))

        from sales_clusterization.reporting.export_resumo_clusters_cep import exportar_resumo_clusters
        exportar_resumo_clusters(self.tenant_id, clusterization_id)

        duracao = round(time.time() - inicio_execucao, 2)
        logger.success(
            f"🏁 Clusterização balanceada concluída com sucesso | clusterization_id={clusterization_id}"
        )

        return {
            "status": "done",
            "tenant_id": self.tenant_id,
            "uf": self.uf,
            "input_id": self.input_id,
            "descricao": f"{self.descricao} (balanceada)",
            "clusterization_id": clusterization_id,
            "duracao_segundos": duracao,
            "min_ceps": self.min_ceps,
            "max_ceps": self.max_ceps,
            "max_merge_km": self.max_merge_km,
        }

    # ============================================================
    # 🔽 FUNÇÃO DE LEITURA SQL ROBUSTA (SUBSTITUI pd.read_sql)
    # ============================================================
    def _read_df_sql(self, sql: str, params: tuple):
        conn = get_connection()
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        cur.close()
        conn.close()
        return pd.DataFrame(rows, columns=cols)

    # ============================================================
    # ⚖️ BALANCEAMENTO PRINCIPAL
    # ============================================================
    def balancear_clusters(self, df_ceps: pd.DataFrame, df_centros: pd.DataFrame) -> pd.DataFrame:
        logger.info("⚖️ Iniciando balanceamento realista de CEPs (min/max real).")

        filtro_coluna = "clientes_total" if getattr(self, "usar_clientes_total", False) else "clientes_target"
        df_ceps = df_ceps[df_ceps[filtro_coluna] > 0].copy()

        OUTLIER_MAX_KM = 30
        df_ceps["distancia_km"] = df_ceps.apply(
            lambda r: haversine((r["lat"], r["lon"]), (r["cluster_lat"], r["cluster_lon"])),
            axis=1
        )

        antes = len(df_ceps)
        df_ceps = df_ceps[df_ceps["distancia_km"] <= OUTLIER_MAX_KM].copy()
        removidos = antes - len(df_ceps)

        if removidos > 0:
            logger.warning(f"🧹 Removidos {removidos} outliers (> {OUTLIER_MAX_KM} km).")

        centros_dict = df_centros.set_index("cluster_id").to_dict("index")
        total_mov = 0
        warnings_total = []

        # ============================================================
        # 🔁 Iterações de ajuste
        # ============================================================
        for i in range(1, self.max_iter + 1):
            resumo = df_ceps.groupby("cluster_id")["cep"].count().reset_index()
            resumo = resumo.rename(columns={"cep": "qtd_ceps"})
            acima = resumo[resumo["qtd_ceps"] > self.max_ceps]
            abaixo = resumo[resumo["qtd_ceps"] < self.min_ceps]

            logger.info(f"🔁 Iteração {i} — acima={len(acima)}, abaixo={len(abaixo)}")

            if acima.empty and abaixo.empty:
                logger.success("✅ Tudo dentro de min/max.")
                break

            df_ceps, a1, w1 = self.redistribuir_clusters_reais(
                df_ceps, df_centros, centros_dict, self.min_ceps, self.max_ceps, self.max_merge_km
            )

            df_ceps, a2, w2 = self.fundir_clusters_pequenos_reais(
                df_ceps, df_centros, centros_dict, self.min_ceps, self.max_ceps
            )

            mov = a1 + a2
            total_mov += mov
            warnings_total.extend(w1 + w2)

            if mov == 0:
                logger.warning("⚠️ Nenhuma realocação possível nesta iteração.")
                break

            df_centros = df_ceps.groupby("cluster_id")[["lat", "lon"]].mean().reset_index()

        df_ceps = self._corrigir_inconsistencias(df_ceps, df_centros)

        # ============================================================
        # RECÁLCULO FINAL
        # ============================================================
        df_ceps["distancia_km"] = df_ceps.apply(
            lambda r: haversine((r["lat"], r["lon"]), (r["cluster_lat"], r["cluster_lon"])),
            axis=1
        )
        df_ceps["tempo_min"] = df_ceps["distancia_km"] * (60 / 40)

        logger.success(f"🏁 Balanceamento concluído — {total_mov} realocações.")
        return df_ceps


    # ============================================================
    # ♻️ Redistribuição entre centros reais (grandes → médios)
    # ============================================================
    def redistribuir_clusters_reais(self, df_ceps, df_centros, centros_dict, min_ceps, max_ceps, max_merge_km):
        alteracoes = 0
        warnings = []
        cluster_stats = df_ceps.groupby("cluster_id")["cep"].count().to_dict()

        for cluster_id, total_ceps in cluster_stats.items():
            if total_ceps <= max_ceps:
                continue

            excedente = total_ceps - max_ceps
            centro_ref = df_centros[df_centros["cluster_id"] == cluster_id]
            if centro_ref.empty:
                continue

            lat_c, lon_c = centro_ref.iloc[0]["lat"], centro_ref.iloc[0]["lon"]

            df_centros["dist_km"] = df_centros.apply(
                lambda r: haversine((lat_c, lon_c), (r["lat"], r["lon"])), axis=1
            )
            vizinhos = df_centros[
                (df_centros["cluster_id"] != cluster_id)
                & (df_centros["dist_km"] <= max_merge_km)
            ].copy()

            vizinhos["qtd_ceps_atual"] = vizinhos["cluster_id"].map(cluster_stats).fillna(0)
            vizinhos["capacidade_disp"] = max_ceps - vizinhos["qtd_ceps_atual"]
            vizinhos = vizinhos[vizinhos["capacidade_disp"] > 0].sort_values("dist_km")

            ceps_cluster = df_ceps[df_ceps["cluster_id"] == cluster_id].copy()
            ceps_cluster["dist_centro"] = ceps_cluster.apply(
                lambda r: haversine((r["lat"], r["lon"]), (lat_c, lon_c)), axis=1
            )
            ceps_para_mover = ceps_cluster.sort_values("dist_centro", ascending=False)

            if vizinhos.empty:
                warnings.append(f"🚨 Cluster {cluster_id} ({total_ceps}) sem centros viáveis no raio de {max_merge_km} km.")
                continue

            for _, viz in vizinhos.iterrows():
                if excedente <= 0:
                    break

                mover_n = min(excedente, int(viz["capacidade_disp"]))
                mover_df = ceps_para_mover.head(mover_n)
                if mover_df.empty:
                    continue

                novo_id = viz["cluster_id"]
                novo_centro = centros_dict.get(novo_id, {})
                mask = mover_df.index

                for col, val in novo_centro.items():
                    if col in df_ceps.columns:
                        df_ceps.loc[mask, col] = val
                df_ceps.loc[mask, "cluster_id"] = novo_id

                excedente -= mover_n
                alteracoes += mover_n

                logger.info(f"🔁 Movidos {mover_n} CEPs de {cluster_id} → {novo_id} ({viz['dist_km']:.1f} km). Restante={excedente}")

            if excedente > 0:
                warnings.append(f"⚠️ Cluster {cluster_id} manteve {excedente} CEPs excedentes (sem capacidade próxima).")

        return df_ceps, alteracoes, warnings

    # ============================================================
    # 🤝 Fusão de clusters pequenos com centros reais
    # ============================================================
    def fundir_clusters_pequenos_reais(self, df_ceps, df_centros, centros_dict, min_ceps, max_ceps):
        alteracoes = 0
        warnings = []

        cluster_stats = df_ceps.groupby("cluster_id")["cep"].count().to_dict()

        for cluster_id, total in cluster_stats.items():
            # Não precisa fundir
            if total >= min_ceps:
                continue

            # Centro de referência
            centro_ref = df_centros[df_centros["cluster_id"] == cluster_id]
            if centro_ref.empty:
                continue

            lat_c, lon_c = centro_ref.iloc[0]["lat"], centro_ref.iloc[0]["lon"]

            # Calcula distância para todos os centros
            df_centros["dist_km"] = df_centros.apply(
                lambda r: haversine((lat_c, lon_c), (r["lat"], r["lon"])),
                axis=1
            )

            # Centros candidatos (todos menos ele mesmo)
            candidatos = df_centros[df_centros["cluster_id"] != cluster_id].copy()

            # Nenhum vizinho disponível → não funde
            if candidatos.empty:
                warnings.append(
                    f"⚠️ Cluster {cluster_id} ({total}) não pode ser fundido — nenhum vizinho disponível."
                )
                continue

            # Vizinho mais próximo
            candidatos = candidatos.sort_values("dist_km")
            vizinho = candidatos.iloc[0]

            novo_id = vizinho["cluster_id"]
            novo_centro = centros_dict.get(novo_id, {})

            mask = df_ceps["cluster_id"] == cluster_id

            # Atualiza dados de centro
            for col, val in novo_centro.items():
                if col in df_ceps.columns:
                    df_ceps.loc[mask, col] = val

            # Atualiza o cluster
            df_ceps.loc[mask, "cluster_id"] = novo_id

            alteracoes += total
            novo_total = df_ceps[df_ceps["cluster_id"] == novo_id].shape[0]

            # Checagem de estouro
            if novo_total > max_ceps:
                warnings.append(
                    f"⚠️ Cluster {cluster_id} ({total}) fundido a {novo_id} → destino excedeu ({novo_total}>{max_ceps})."
                )
            else:
                logger.info(
                    f"🤝 Cluster {cluster_id} ({total}) fundido com {novo_id} ({vizinho['dist_km']:.1f} km)."
                )

        return df_ceps, alteracoes, warnings


    # ============================================================
    # 🧹 Correção de inconsistências antes de gravação
    # ============================================================
    def _corrigir_inconsistencias(self, df_ceps, df_centros):
        colunas_existentes = [c for c in ["cluster_id", "lat", "lon", "centro_nome", "centro_cnpj", "cluster_bairro"] if c in df_centros.columns]

        df_merged = df_ceps.merge(
            df_centros[colunas_existentes],
            on="cluster_id",
            how="left",
            suffixes=("", "_corrigido"),
        )

        for col in ["lat", "lon", "centro_nome", "centro_cnpj", "cluster_bairro"]:
            if f"{col}_corrigido" in df_merged.columns:
                df_merged[col] = df_merged[f"{col}_corrigido"].fillna(df_merged[col])
                df_merged.drop(columns=[f"{col}_corrigido"], inplace=True, errors="ignore")

        logger.info("✅ Consistência de centros verificada e corrigida antes da gravação.")
        return df_merged

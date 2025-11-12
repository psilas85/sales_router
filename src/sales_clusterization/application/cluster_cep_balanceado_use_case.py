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
    """
    Clusterização balanceada de CEPs com base em limites mínimos e máximos de CEPs.
    Trabalha apenas com centros logísticos reais — não cria novos centros.
    - Clusters grandes redistribuem excedentes entre centros próximos (até max_merge_km).
    - Clusters pequenos fundem-se ao centro real mais próximo, ignorando o raio.
    - Mantém consistência total dos dados de centro (nome, CNPJ, bairro, coordenadas).
    """

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

        conn = get_connection()
        df_ceps = pd.read_sql(
            "SELECT * FROM mkp_cluster_cep WHERE clusterization_id = %s;",
            conn,
            params=[clusterization_id_base],
        )
        conn.close()

        if df_ceps.empty:
            logger.error("❌ Nenhum CEP encontrado para balanceamento.")
            return None

        # Centros reais de referência
        df_centros = (
            df_ceps.groupby("cluster_id")
            .agg(
                lat=("cluster_lat", "mean"),
                lon=("cluster_lon", "mean"),
                centro_nome=("centro_nome", "first"),
                centro_cnpj=("centro_cnpj", "first"),
                cluster_bairro=("cluster_bairro", "first"),
            )
            .reset_index()
        )

        df_bal = self.balancear_clusters(df_ceps.copy(), df_centros.copy())
        df_bal["modo_clusterizacao"] = "balanceada"
        df_bal["clusterization_id"] = clusterization_id

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
    # ⚖️ Balanceamento principal
    # ============================================================
    def balancear_clusters(self, df_ceps: pd.DataFrame, df_centros: pd.DataFrame) -> pd.DataFrame:
        logger.info("⚖️ Iniciando balanceamento de clusters com base em CEPs (modo realista).")

        filtro_coluna = "clientes_total" if getattr(self, "usar_clientes_total", False) else "clientes_target"
        df_ceps = df_ceps[df_ceps[filtro_coluna] > 0].copy()
        logger.info(f"📊 Filtro aplicado: apenas CEPs com {filtro_coluna} > 0 ({len(df_ceps)} linhas válidas).")

        # Cria dicionário fixo com dados dos centros
        centros_dict = df_centros.set_index("cluster_id").to_dict("index")

        alteracoes_total = 0
        warnings_total = []

        for iteracao in range(1, self.max_iter + 1):
            resumo = df_ceps.groupby("cluster_id")["cep"].count().reset_index().rename(columns={"cep": "qtd_ceps"})
            clusters_acima = resumo[resumo["qtd_ceps"] > self.max_ceps]
            clusters_abaixo = resumo[resumo["qtd_ceps"] < self.min_ceps]

            logger.info(f"🔁 Iteração {iteracao}/{self.max_iter} — acima={len(clusters_acima)} | abaixo={len(clusters_abaixo)}")

            if clusters_acima.empty and clusters_abaixo.empty:
                logger.success("✅ Todos os clusters dentro dos limites definidos.")
                break

            df_ceps, alter_excesso, warn_excesso = self.redistribuir_clusters_reais(
                df_ceps, df_centros, centros_dict, self.min_ceps, self.max_ceps, self.max_merge_km
            )

            df_ceps, alter_deficit, warn_deficit = self.fundir_clusters_pequenos_reais(
                df_ceps, df_centros, centros_dict, self.min_ceps, self.max_ceps
            )

            alter_iter = alter_excesso + alter_deficit
            alteracoes_total += alter_iter
            warnings_total.extend(warn_excesso + warn_deficit)

            if alter_iter == 0:
                logger.warning("⚠️ Nenhuma realocação adicional possível nesta iteração.")
                break

            # Atualiza centros médios (sem alterar dados originais)
            df_centros = (
                df_ceps.groupby("cluster_id")[["lat", "lon"]]
                .mean()
                .reset_index()
            )

        # Verificação final de consistência
        df_ceps = self._corrigir_inconsistencias(df_ceps, df_centros)

        resumo_final = df_ceps.groupby("cluster_id")["cep"].count()
        p90 = np.percentile(resumo_final, 90)
        logger.info(
            f"📈 Estatísticas finais — Clusters: {df_ceps['cluster_id'].nunique()} | "
            f"min={resumo_final.min()} | média={resumo_final.mean():.1f} | "
            f"máx={resumo_final.max()} | p90={p90:.1f}"
        )

        if warnings_total:
            logger.warning("⚠️ Clusters com avisos:")
            for w in warnings_total:
                logger.warning(f"   • {w}")

        logger.success(f"🏁 Balanceamento concluído: {alteracoes_total} CEPs realocados.")
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
            if total >= min_ceps:
                continue

            centro_ref = df_centros[df_centros["cluster_id"] == cluster_id]
            if centro_ref.empty:
                continue

            lat_c, lon_c = centro_ref.iloc[0]["lat"], centro_ref.iloc[0]["lon"]

            df_centros["dist_km"] = df_centros.apply(
                lambda r: haversine((lat_c, lon_c), (r["lat"], r["lon"])), axis=1
            )
            vizinho = df_centros[df_centros["cluster_id"] != cluster_id].sort_values("dist_km").iloc[0]
            novo_id = vizinho["cluster_id"]
            novo_centro = centros_dict.get(novo_id, {})

            mask = df_ceps["cluster_id"] == cluster_id
            for col, val in novo_centro.items():
                if col in df_ceps.columns:
                    df_ceps.loc[mask, col] = val
            df_ceps.loc[mask, "cluster_id"] = novo_id

            alteracoes += total
            novo_total = df_ceps[df_ceps["cluster_id"] == novo_id].shape[0]

            if novo_total > max_ceps:
                warnings.append(f"⚠️ Cluster {cluster_id} ({total}) fundido a {novo_id} → destino excedeu ({novo_total}>{max_ceps}).")
            else:
                logger.info(f"🤝 Cluster {cluster_id} ({total}) fundido com {novo_id} ({vizinho['dist_km']:.1f} km).")

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

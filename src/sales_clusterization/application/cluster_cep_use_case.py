# ============================================================
# 📦 src/sales_clusterization/application/cluster_cep_use_case.py
# ============================================================

from loguru import logger as logging
import pandas as pd
import numpy as np
import uuid
from sklearn.cluster import KMeans
from sales_clusterization.domain.haversine_utils import haversine
from pdv_preprocessing.domain.geolocation_service import GeolocationService


class ClusterCEPUseCase:
    """
    Clusterização de CEPs do marketplace.
    - Baseia-se em lat/lon e restrição de tempo máximo do centro até o CEP.
    - Usa KMeans com estimativa automática de K inicial e ajuste iterativo.
    - Aplica jitter leve opcional para diferenciar CEPs duplicados.
    """

    def __init__(self, reader, writer, tenant_id, uf, input_id, descricao,
                 velocidade_media, tempo_max_min,
                 usar_clientes_target=False,
                 excluir_outliers=False,
                 cidade=None,
                 ajustar_coordenadas=True):
        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.uf = uf
        self.input_id = input_id
        self.descricao = descricao
        self.velocidade_media = velocidade_media
        self.tempo_max_min = tempo_max_min
        self.usar_clientes_target = usar_clientes_target
        self.excluir_outliers = excluir_outliers
        self.cidade = cidade
        self.ajustar_coordenadas = ajustar_coordenadas

        # GeolocationService agora com reader e writer injetados
        self.geo_service = GeolocationService(reader=self.reader, writer=self.writer)

    # ------------------------------------------------------------
    # Execução principal
    # ------------------------------------------------------------
    def execute(self, ceps_max_cluster: int = None):
        # ============================================================
        # 🆕 clusterization_id
        # ============================================================
        clusterization_id = str(uuid.uuid4())
        logging.info(f"🚀 Iniciando clusterização de CEPs (tenant={self.tenant_id}, UF={self.uf})")
        logging.info(f"🆕 clusterization_id gerado: {clusterization_id}")

        # ============================================================
        # 📥 Carrega dados do marketplace
        # ============================================================
        registros = self.reader.buscar_marketplace_ceps(
            self.tenant_id, self.uf, self.input_id, self.cidade
        )
        if not registros:
            logging.warning("⚠️ Nenhum registro marketplace_cep encontrado.")
            return None

        df = pd.DataFrame(
            registros,
            columns=["cep", "lat", "lon", "clientes_total", "clientes_target"]
        )

        total_inicial = len(df)

        # ============================================================
        # 🏘️ (Etapa desativada) Preenchimento de bairros
        # ============================================================
        logging.info("🏘️ Etapa de preenchimento de bairros desativada — seguindo apenas com cidade, UF e coordenadas.")
        # Nenhuma chamada de reverse geocode será feita nesta etapa.

        # ============================================================
        # 🎯 Seleção de peso e filtro condicional
        # ============================================================
        if self.usar_clientes_target:
            df = df[df["clientes_target"].fillna(0) > 0].copy()
            coluna_peso = "clientes_target"
            logging.info(f"📊 Utilizando coluna de peso: {coluna_peso} (filtrando apenas clientes_target > 0)")
        else:
            df = df[df["clientes_total"].fillna(0) > 0].copy()
            coluna_peso = "clientes_total"
            logging.info(f"📊 Utilizando coluna de peso: {coluna_peso}")

        total_pos_filtro = len(df)
        logging.info(f"📦 Registros antes do filtro: {total_inicial} → após filtro: {total_pos_filtro}")

        # ============================================================
        # ⚖️ Normalização de pesos (0–1)
        # ============================================================
        df["peso"] = df[coluna_peso].astype(float)
        df["peso_norm"] = df["peso"] / df["peso"].max() if df["peso"].max() > 0 else 1.0
        logging.info(f"⚖️ Pesos normalizados entre {df['peso_norm'].min():.2f} e {df['peso_norm'].max():.2f}")

        # ============================================================
        # 🌍 Aplica jitter leve em coordenadas duplicadas (±0.002°)
        # ============================================================
        if self.ajustar_coordenadas:
            duplicadas = df.groupby(["lat", "lon"]).size()
            qtd_dups = (duplicadas > 1).sum()
            if qtd_dups > 0:
                logging.info(f"✨ Aplicando jitter em {qtd_dups} coordenadas duplicadas...")
                df["lat"] += np.random.uniform(-0.002, 0.002, size=len(df))
                df["lon"] += np.random.uniform(-0.002, 0.002, size=len(df))
            else:
                logging.info("✅ Nenhuma coordenada duplicada — jitter não necessário.")

        # ============================================================
        # 🧮 Estimativa inicial de K
        # ============================================================
        lat_min, lat_max = df["lat"].min(), df["lat"].max()
        lon_min, lon_max = df["lon"].min(), df["lon"].max()
        diametro_km = haversine((lat_min, lon_min), (lat_max, lon_max))
        raio_km_max = (self.velocidade_media * self.tempo_max_min) / 60.0

        area_total = np.pi * (diametro_km / 2) ** 2
        area_cluster = np.pi * raio_km_max ** 2
        k_inicial = max(1, int(area_total / area_cluster))
        k_inicial = min(k_inicial, len(df) // 3) or 1

        logging.info(
            f"🧩 K inicial estimado: {k_inicial} "
            f"(raio máx={raio_km_max:.2f} km, tempo máx={self.tempo_max_min} min)"
        )

        # ============================================================
        # 🔁 Loop adaptativo: tempo e quantidade de CEPs
        # ============================================================
        k_atual = k_inicial
        max_iter = 10
        for tentativa in range(max_iter):
            logging.info(f"🔄 Tentativa {tentativa+1}/{max_iter}: executando KMeans com k={k_atual}")
            coords = df[["lat", "lon"]].values
            kmeans = KMeans(n_clusters=k_atual, random_state=42, n_init=10)
            df["cluster_id"] = kmeans.fit_predict(coords)

            tempo_max_global = 0
            ceps_max_global = 0

            for cluster_id in sorted(df["cluster_id"].unique()):
                grupo = df[df["cluster_id"] == cluster_id].copy()
                centro_lat = np.average(grupo["lat"], weights=grupo["peso_norm"])
                centro_lon = np.average(grupo["lon"], weights=grupo["peso_norm"])
                grupo["distancia_km"] = grupo.apply(
                    lambda r: haversine((r["lat"], r["lon"]), (centro_lat, centro_lon)), axis=1
                )
                grupo["tempo_min"] = (grupo["distancia_km"] / self.velocidade_media) * 60
                tempo_max_cluster = grupo["tempo_min"].max()
                tempo_max_global = max(tempo_max_global, tempo_max_cluster)
                ceps_max_global = max(ceps_max_global, len(grupo))

            tempo_ok = tempo_max_global <= self.tempo_max_min
            ceps_ok = (ceps_max_cluster is None) or (ceps_max_global <= ceps_max_cluster)

            logging.info(
                f"⏱️ Tempo máx global={tempo_max_global:.2f} min | "
                f"CEPs máx cluster={ceps_max_global} | k={k_atual}"
            )

            if tempo_ok and ceps_ok:
                logging.info("✅ Critérios atendidos: tempo e quantidade de CEPs.")
                break

            if tentativa == max_iter - 1:
                logging.warning(f"⚠️ Limite de iterações atingido (k={k_atual}) — encerrando.")
                break

            k_atual += 1
            logging.info(f"⚙️ Aumentando K para {k_atual} e recalculando...")

        # ============================================================
        # 💾 Geração e gravação final
        # ============================================================
        lista_clusters = []
        for cluster_id in sorted(df["cluster_id"].unique()):
            grupo = df[df["cluster_id"] == cluster_id].copy()
            centro_lat = np.average(grupo["lat"], weights=grupo["peso_norm"])
            centro_lon = np.average(grupo["lon"], weights=grupo["peso_norm"])

            # ============================================================
            # 🧭 Cálculo de distâncias e métricas do cluster
            # ============================================================
            grupo["distancia_km"] = grupo.apply(
                lambda r: haversine((r["lat"], r["lon"]), (centro_lat, centro_lon)), axis=1
            )
            grupo["tempo_min"] = (grupo["distancia_km"] / self.velocidade_media) * 60
            grupo["is_outlier"] = grupo["tempo_min"] > self.tempo_max_min

            qtd_ceps = len(grupo)
            outliers = grupo["is_outlier"].sum()
            logging.info(
                f"📍 Cluster {cluster_id}: {qtd_ceps} CEPs | "
                f"Centro=({centro_lat:.5f}, {centro_lon:.5f}) | Outliers={outliers}"
            )

            for _, row in grupo.iterrows():
                lista_clusters.append({
                    "tenant_id": self.tenant_id,
                    "input_id": self.input_id,
                    "clusterization_id": clusterization_id,
                    "uf": self.uf,
                    "cep": row["cep"],
                    "cluster_id": int(cluster_id),
                    "clientes_total": int(row["clientes_total"]),
                    "clientes_target": int(row["clientes_target"]),
                    "cluster_lat": float(centro_lat),
                    "cluster_lon": float(centro_lon),
                    "distancia_km": float(row["distancia_km"]),
                    "tempo_min": float(row["tempo_min"]),
                    "is_outlier": bool(row["is_outlier"]),
                })

        inseridos = self.writer.inserir_mkp_cluster_cep(lista_clusters)
        total_clusters = df["cluster_id"].nunique()
        total_ceps = len(df)
        total_outliers = df["is_outlier"].sum() if "is_outlier" in df.columns else 0

        logging.info(
            f"✅ {inseridos} registros gravados em mkp_cluster_cep | "
            f"Clusters={total_clusters} | CEPs={total_ceps} | Outliers={total_outliers}"
        )
        logging.success(f"🏁 Clusterização finalizada com sucesso | clusterization_id={clusterization_id}")

        # ============================================================
        # 📊 Gera resumo automático (CSV)
        # ============================================================
        try:
            from sales_clusterization.reporting.export_resumo_clusters_cep import exportar_resumo_clusters
            logging.info("📈 Gerando resumo de clusters (CSV)...")
            exportar_resumo_clusters(self.tenant_id, clusterization_id)
        except Exception as e:
            logging.warning(f"⚠️ Falha ao gerar resumo automático: {e}")

        return clusterization_id

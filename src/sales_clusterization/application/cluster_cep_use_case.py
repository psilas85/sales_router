# ============================================================
# 📦 src/sales_clusterization/application/cluster_cep_use_case.py
# ============================================================

from loguru import logger as logging
import pandas as pd
import numpy as np
import uuid
from sklearn.cluster import KMeans
from sales_clusterization.domain.haversine_utils import haversine


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

    # ------------------------------------------------------------
    # Execução principal
    # ------------------------------------------------------------
    def execute(self):
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
        coluna_peso = "clientes_target" if self.usar_clientes_target else "clientes_total"
        logging.info(f"📊 Utilizando coluna de peso: {coluna_peso}")

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
        # 🤖 KMeans inicial
        # ============================================================
        coords = df[["lat", "lon"]].values
        kmeans = KMeans(n_clusters=k_inicial, random_state=42, n_init=10)
        df["cluster_id"] = kmeans.fit_predict(coords)
        centros = kmeans.cluster_centers_

        logging.info(f"📈 Clusters únicos identificados: {df['cluster_id'].nunique()}")

        # ============================================================
        # 🔁 Cálculo de distâncias e tempos
        # ============================================================
        lista_clusters = []
        for cluster_id in sorted(df["cluster_id"].unique()):
            grupo = df[df["cluster_id"] == cluster_id].copy()
            centro = centros[int(cluster_id)]

            grupo["distancia_km"] = grupo.apply(
                lambda r: haversine((r["lat"], r["lon"]), centro), axis=1
            )
            grupo["tempo_min"] = (grupo["distancia_km"] / self.velocidade_media) * 60
            grupo["is_outlier"] = grupo["tempo_min"] > self.tempo_max_min

            qtd_ceps = len(grupo)
            outliers = grupo["is_outlier"].sum()
            logging.info(
                f"📍 Cluster {cluster_id}: {qtd_ceps} CEPs | "
                f"Centro=({centro[0]:.5f}, {centro[1]:.5f}) | "
                f"Outliers={outliers}"
            )

            for _, row in grupo.iterrows():
                lista_clusters.append({
                    "tenant_id": self.tenant_id,
                    "input_id": self.input_id,
                    "clusterization_id": clusterization_id,  # ✅ novo campo
                    "uf": self.uf,
                    "cep": row["cep"],
                    "cluster_id": int(cluster_id),
                    "clientes_total": int(row["clientes_total"]),
                    "clientes_target": int(row["clientes_target"]),
                    "cluster_lat": float(centro[0]),
                    "cluster_lon": float(centro[1]),
                    "distancia_km": float(row["distancia_km"]),
                    "tempo_min": float(row["tempo_min"]),
                    "is_outlier": bool(row["is_outlier"]),
                })

        # ============================================================
        # 💾 Salva resultado no banco
        # ============================================================
        inseridos = self.writer.inserir_mkp_cluster_cep(lista_clusters)
        total_clusters = df["cluster_id"].nunique()
        total_ceps = len(df)
        total_outliers = df["is_outlier"].sum() if "is_outlier" in df.columns else 0

        logging.info(
            f"✅ {inseridos} registros gravados em mkp_cluster_cep | "
            f"Clusters={total_clusters} | CEPs={total_ceps} | Outliers={total_outliers}"
        )
        logging.success(f"🏁 Clusterização finalizada com sucesso | clusterization_id={clusterization_id}")

        return clusterization_id

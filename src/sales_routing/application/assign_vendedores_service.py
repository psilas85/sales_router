# src/sales_routing/application/assign_vendedores_service.py

import math
import pandas as pd
from sklearn.cluster import KMeans
from loguru import logger
from src.sales_routing.infrastructure.database_reader import SalesRoutingDatabaseReader
from src.sales_routing.infrastructure.database_writer import SalesRoutingDatabaseWriter


class AssignVendedoresService:
    """
    Serviço de atribuição espacial de vendedores às rotas já salvas.
    - Lê rotas operacionais do banco (sales_subcluster)
    - Clusteriza centroides para definir territórios de vendedor
    - Atualiza registros com vendedor_id (persistência otimizada)
    """

    def __init__(self, tenant_id: int, freq_mensal: int = 4, dias_uteis: int = 20):
        self.tenant_id = tenant_id
        self.freq_mensal = freq_mensal
        self.dias_uteis = dias_uteis
        self.db_reader = SalesRoutingDatabaseReader()
        self.db_writer = SalesRoutingDatabaseWriter()

    # =========================================================
    # 1️⃣ Calcula capacidade e necessidade
    # =========================================================
    def _calcular_parametros(self, total_rotas: int):
        """Define quantas rotas cada vendedor pode atender e quantos são necessários."""
        rotas_por_vendedor = max(1, round(self.dias_uteis / self.freq_mensal))
        vendedores_necessarios = math.ceil(total_rotas / rotas_por_vendedor)
        vendedores_necessarios = min(vendedores_necessarios, total_rotas)

        logger.info(f"🧮 Freq. mensal: {self.freq_mensal}x | Dias úteis: {self.dias_uteis}")
        logger.info(f"👥 Cada vendedor pode atender até {rotas_por_vendedor} rotas.")
        logger.info(f"📊 Total de rotas: {total_rotas} → Necessários {vendedores_necessarios} vendedores.")
        return rotas_por_vendedor, vendedores_necessarios

    # =========================================================
    # 2️⃣ Clusteriza centroides das rotas (KMeans)
    # =========================================================
    def _atribuir_por_kmeans(self, rotas_df: pd.DataFrame, vendedores_necessarios: int):
        """Executa clusterização espacial para atribuir vendedor_id baseado em centroides."""
        coords = rotas_df[["centro_lat", "centro_lon"]].values
        modelo = KMeans(n_clusters=vendedores_necessarios, random_state=42, n_init="auto")
        rotas_df["vendedor_id"] = modelo.fit_predict(coords) + 1
        return rotas_df

    # =========================================================
    # 3️⃣ Execução principal
    # =========================================================
    def executar(self, uf: str = None, cidade: str = None):
        """Executa a atribuição de vendedores, com filtros opcionais por UF e cidade."""
        filtro_txt = ""
        if uf and cidade:
            filtro_txt = f" (UF={uf}, Cidade={cidade})"
        elif uf:
            filtro_txt = f" (UF={uf})"

        logger.info(f"🏁 Iniciando atribuição de vendedores (tenant={self.tenant_id}){filtro_txt}...")

        # 🔹 Carrega rotas do banco com filtro opcional
        rotas = self.db_reader.get_operational_routes(self.tenant_id, uf=uf, cidade=cidade)
        if not rotas:
            logger.warning("❌ Nenhuma rota operacional encontrada para este filtro.")
            return None, 0, 0

        rotas_df = pd.DataFrame(rotas)
        total_rotas = len(rotas_df)
        logger.info(f"📦 {total_rotas} rotas carregadas para atribuição.")

        # 🔹 Validação de coordenadas
        rotas_df = rotas_df.dropna(subset=["centro_lat", "centro_lon"])
        if rotas_df.empty:
            logger.error("❌ Nenhuma rota possui coordenadas válidas (centro_lat/lon).")
            return None, 0, 0

        rotas_invalidas = total_rotas - len(rotas_df)
        if rotas_invalidas > 0:
            logger.warning(f"⚠️ {rotas_invalidas} rotas removidas por falta de coordenadas.")

        # 🔹 Calcula necessidade
        rotas_por_vendedor, vendedores_necessarios = self._calcular_parametros(len(rotas_df))

        # 🔹 Clusteriza centroides → vendedor_id
        rotas_df = self._atribuir_por_kmeans(rotas_df, vendedores_necessarios)

        # 🔹 Atualiza no banco (batch update otimizado)
        self.db_writer.update_vendedores_operacional(
            tenant_id=self.tenant_id,
            rotas=rotas_df[["id", "vendedor_id"]].to_dict(orient="records")
        )

        # 🔹 Estatísticas por vendedor
        resumo = rotas_df.groupby("vendedor_id").agg({
            "id": "count",
            "n_pdvs": "sum",
            "dist_total_km": "sum",
            "tempo_total_min": "sum"
        }).rename(columns={
            "id": "rotas",
            "n_pdvs": "total_pdvs",
            "dist_total_km": "dist_km",
            "tempo_total_min": "tempo_min"
        }).reset_index()

        logger.info("📈 Distribuição por vendedor:")
        for _, row in resumo.iterrows():
            logger.info(f"   🧍‍♂️ Vendedor {int(row.vendedor_id)} → "
                        f"{int(row.rotas)} rotas | {int(row.total_pdvs)} PDVs | "
                        f"{row.dist_km:.1f} km | {row.tempo_min:.1f} min")

        logger.success(f"✅ {vendedores_necessarios} vendedores atribuídos e salvos no banco (tenant={self.tenant_id}).")
        return rotas_df, vendedores_necessarios, rotas_por_vendedor

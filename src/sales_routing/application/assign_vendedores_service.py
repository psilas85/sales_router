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

    def __init__(self, tenant_id: int, freq_mensal: int = 1, dias_uteis: int = 20, workday_min: int = 500):
        """
        Inicializa o serviço de atribuição de vendedores.
        - tenant_id: identificador do tenant
        - freq_mensal: frequência média de visitas (vezes/mês)
        - dias_uteis: total de dias úteis no mês
        - workday_min: limite diário de trabalho em minutos
        """
        self.tenant_id = tenant_id
        self.freq_mensal = freq_mensal
        self.dias_uteis = dias_uteis
        self.workday_min = workday_min
        self.db_reader = SalesRoutingDatabaseReader()
        self.db_writer = SalesRoutingDatabaseWriter()

    # =========================================================
    # 1️⃣ Calcula capacidade e necessidade teórica
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
    # 2️⃣ Atribuição geográfica (proximidade + limite de rotas)
    # =========================================================

    # dentro de AssignVendedoresService

    def _atribuir_por_sweep(self, rotas_df: pd.DataFrame, eixo: str = "lon"):
        """
        Atribui rotas por varredura geográfica (sweep):
        - Ordena rotas por eixo (lon -> Oeste→Leste; lat -> Sul→Norte)
        - Para cada vendedor: inicia com a próxima rota não atribuída e
        vai anexando a rota mais próxima do centróide atual até atingir a capacidade.
        """
        import numpy as np
        from math import radians, sin, cos, sqrt, atan2

        # capacidade (rotas por vendedor)
        cap = max(1, round(self.dias_uteis / self.freq_mensal))

        def hav(lat1, lon1, lat2, lon2):
            R = 6371.0
            dlat = radians(lat2 - lat1)
            dlon = radians(lon2 - lon1)
            a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
            return 2 * R * atan2(sqrt(a), sqrt(1 - a))

        # ordenação de varredura
        if eixo == "lat":
            rotas_df = rotas_df.sort_values(by=["centro_lat", "centro_lon"]).reset_index(drop=True)
        else:  # "lon" default
            rotas_df = rotas_df.sort_values(by=["centro_lon", "centro_lat"]).reset_index(drop=True)

        n = len(rotas_df)
        vendedor_id = 0
        rotas_df["vendedor_id"] = np.nan
        assigned = np.zeros(n, dtype=bool)

        # pré-vetores para speed
        lats = rotas_df["centro_lat"].to_numpy()
        lons = rotas_df["centro_lon"].to_numpy()

        while not assigned.all():
            # nova "semente": próxima rota não atribuída na ordem de varredura
            seed_idx = int(np.where(~assigned)[0][0])
            vendedor_id += 1
            grupo = [seed_idx]
            assigned[seed_idx] = True

            # centróide atual do vendedor
            cen_lat = float(lats[seed_idx])
            cen_lon = float(lons[seed_idx])

            # completa até a capacidade
            while len(grupo) < cap and not assigned.all():
                # distâncias das não atribuídas ao centróide
                cand_idx = np.where(~assigned)[0]
                dists = [
                    hav(cen_lat, cen_lon, float(lats[i]), float(lons[i]))
                    for i in cand_idx
                ]
                if not dists:
                    break
                # escolhe a mais próxima
                j = int(cand_idx[int(np.argmin(dists))])
                grupo.append(j)
                assigned[j] = True
                # atualiza centróide do grupo
                cen_lat = float(lats[grupo].mean())
                cen_lon = float(lons[grupo].mean())

            # grava vendedor_id no DataFrame
            rotas_df.loc[grupo, "vendedor_id"] = vendedor_id

            # log rápido
            if len(grupo) > 0:
                logger.debug(f"🧍‍♂️ Vendedor {vendedor_id} formado com {len(grupo)} rotas.")

        vendedores_necessarios = vendedor_id
        logger.info(f"✅ Atribuição sweep concluída: {vendedores_necessarios} vendedores (capacidade {cap} rotas/vendedor).")
        # garante tipo int
        rotas_df["vendedor_id"] = rotas_df["vendedor_id"].astype(int)
        return rotas_df, vendedores_necessarios

    def _atribuir_por_proximidade(self, rotas_df: pd.DataFrame):
        """
        Atribui rotas a vendedores com base em proximidade geográfica,
        respeitando o limite máximo de rotas por vendedor.
        """
        rotas_max_por_vendedor = max(1, round(self.dias_uteis / self.freq_mensal))
        total_rotas = len(rotas_df)
        vendedores_necessarios = math.ceil(total_rotas / rotas_max_por_vendedor)

        logger.info(f"📍 Atribuição geográfica: até {rotas_max_por_vendedor} rotas por vendedor.")
        logger.info(f"👥 Serão criados {vendedores_necessarios} clusters de vendedores.")

        coords = rotas_df[["centro_lat", "centro_lon"]].values
        modelo = KMeans(n_clusters=vendedores_necessarios, random_state=42, n_init="auto")
        rotas_df["vendedor_id"] = modelo.fit_predict(coords) + 1

        # Balanceamento — redistribui excesso de rotas, se houver
        contagem = rotas_df["vendedor_id"].value_counts()
        excesso = contagem[contagem > rotas_max_por_vendedor]

        if not excesso.empty:
            logger.warning("⚠️ Alguns vendedores ultrapassaram o limite de rotas — redistribuindo...")
            for vid in excesso.index:
                excesso_rotas = contagem[vid] - rotas_max_por_vendedor
                rotas_excesso = rotas_df[rotas_df["vendedor_id"] == vid].sample(excesso_rotas)
                novo_vid = rotas_df["vendedor_id"].max() + 1
                rotas_df.loc[rotas_excesso.index, "vendedor_id"] = novo_vid
                logger.debug(f"♻️ {excesso_rotas} rotas movidas de vendedor {vid} para novo {novo_vid}")

        vendedores_necessarios = rotas_df["vendedor_id"].nunique()
        logger.success(f"✅ Atribuição concluída com {vendedores_necessarios} vendedores.")
        return rotas_df, vendedores_necessarios

    # =========================================================
    # 3️⃣ Cálculo da base ideal (bairro/cidade) de cada vendedor
    # =========================================================
    def calcular_bases_vendedores(self, rotas_df: pd.DataFrame):
        """Calcula base (bairro/cidade) ideal de cada vendedor e salva em tabela."""
        from src.database.db_connection import get_connection_context
        from math import radians, sin, cos, sqrt, atan2

        def _haversine(lat1, lon1, lat2, lon2):
            R = 6371
            dlat = radians(lat2 - lat1)
            dlon = radians(lon2 - lon1)
            a = sin(dlat/2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon/2)**2
            return R * 2 * atan2(sqrt(a), sqrt(1 - a))

        resultados = []

        for vendedor_id, grupo in rotas_df.groupby("vendedor_id"):
            base_lat = grupo["centro_lat"].mean()
            base_lon = grupo["centro_lon"].mean()

            with get_connection_context() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        SELECT 
                            pd.bairro, 
                            pd.cidade,
                            COALESCE(pd.pdv_lat, sp.lat) AS lat,
                            COALESCE(pd.pdv_lon, sp.lon) AS lon
                        FROM sales_subcluster_pdv sp
                        JOIN pdvs pd ON pd.id = sp.pdv_id
                        JOIN sales_subcluster s
                          ON s.cluster_id = sp.cluster_id
                         AND s.subcluster_seq = sp.subcluster_seq
                         AND s.tenant_id = sp.tenant_id
                        WHERE s.tenant_id = %s AND s.vendedor_id = %s;
                    """, (self.tenant_id, vendedor_id))
                    rows = cur.fetchall()

            if not rows:
                continue

            df = pd.DataFrame(rows, columns=["bairro", "cidade", "lat", "lon"])
            df["dist_km"] = df.apply(lambda x: _haversine(base_lat, base_lon, x.lat, x.lon), axis=1)
            df_near = df.sort_values("dist_km").head(50)

            if df_near["bairro"].notna().any():
                base_bairro = (
                    df_near.groupby("bairro")["bairro"]
                    .count()
                    .sort_values(ascending=False)
                    .index[0]
                )
                base_cidade = (
                    df_near[df_near["bairro"] == base_bairro]["cidade"].mode().iat[0]
                    if not df_near[df_near["bairro"] == base_bairro]["cidade"].empty
                    else None
                )
            else:
                base_bairro = None
                base_cidade = df_near["cidade"].mode().iat[0] if not df_near["cidade"].empty else None

            resultados.append({
                "vendedor_id": vendedor_id,
                "base_bairro": base_bairro,
                "base_cidade": base_cidade,
                "base_lat": base_lat,
                "base_lon": base_lon,
                "total_rotas": len(grupo),
                "total_pdvs": grupo["n_pdvs"].sum()
            })

        # Persiste resultados
        from src.database.db_connection import get_connection_context
        with get_connection_context() as conn:
            with conn.cursor() as cur:
                cur.execute("DELETE FROM sales_vendedor_base WHERE tenant_id = %s;", (int(self.tenant_id),))
                for r in resultados:
                    cur.execute("""
                        INSERT INTO sales_vendedor_base
                        (tenant_id, vendedor_id, base_bairro, base_cidade,
                         base_lat, base_lon, total_rotas, total_pdvs, data_calculo)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,NOW());
                    """, (
                        int(self.tenant_id),
                        int(r["vendedor_id"]),
                        str(r["base_bairro"]) if r["base_bairro"] else None,
                        str(r["base_cidade"]) if r["base_cidade"] else None,
                        float(r["base_lat"]) if not pd.isna(r["base_lat"]) else None,
                        float(r["base_lon"]) if not pd.isna(r["base_lon"]) else None,
                        int(r["total_rotas"]) if not pd.isna(r["total_rotas"]) else 0,
                        int(r["total_pdvs"]) if not pd.isna(r["total_pdvs"]) else 0
                    ))

        logger.success(f"✅ {len(resultados)} bases de vendedores (nível bairro) calculadas e salvas.")
        for linha in resultados[:5]:
            logger.info(
                f"🏠 Vendedor {linha['vendedor_id']}: {linha['base_bairro']} / {linha['base_cidade']} "
                f"({linha['base_lat']:.4f}, {linha['base_lon']:.4f})"
            )

    # =========================================================
    # 4️⃣ Execução principal
    # =========================================================
    def executar(self, uf: str = None, cidade: str = None):
        """Executa a atribuição de vendedores com base em proximidade geográfica."""
        from src.database.cleanup_service import limpar_dados_operacionais
        from src.sales_routing.reporting.vendedores_summary_service import VendedoresSummaryService

        # 🔹 Limpa base de vendedores
        logger.info(f"🧹 Limpando base de vendedores antes do processamento (tenant={self.tenant_id})...")
        limpar_dados_operacionais("assign_vendedores", tenant_id=self.tenant_id)

        filtro_txt = f" (UF={uf}, Cidade={cidade})" if cidade else (f" (UF={uf})" if uf else "")
        logger.info(f"🏁 Iniciando atribuição de vendedores (tenant={self.tenant_id}){filtro_txt}...")

        # 🔹 Carrega rotas operacionais
        rotas = self.db_reader.get_operational_routes(self.tenant_id, uf=uf, cidade=cidade)
        if not rotas:
            logger.warning("❌ Nenhuma rota operacional encontrada para este filtro.")
            return None, 0, 0

        rotas_df = pd.DataFrame(rotas)
        rotas_df = rotas_df.dropna(subset=["centro_lat", "centro_lon"])
        total_rotas = len(rotas_df)
        logger.info(f"📦 {total_rotas} rotas carregadas para atribuição.")

        if total_rotas == 0:
            logger.error("❌ Nenhuma rota válida para atribuição.")
            return None, 0, 0

        # 🔹 Atribui vendedores com base em proximidade
        # Oeste → Leste (padrão). Para Sul → Norte, use eixo="lat".
        rotas_df, vendedores_necessarios = self._atribuir_por_sweep(rotas_df, eixo="lon")


        # 🔹 Atualiza banco
        self.db_writer.update_vendedores_operacional(
            tenant_id=self.tenant_id,
            rotas=rotas_df[["id", "vendedor_id"]].to_dict(orient="records")
        )

        # 🔹 Calcula base ideal (bairro/cidade)
        self.calcular_bases_vendedores(rotas_df)

        # 🔹 Gera relatório CSV + JSON
        VendedoresSummaryService(self.tenant_id).gerar_relatorio()

        logger.success(f"✅ {vendedores_necessarios} vendedores atribuídos, bases calculadas e relatório gerado (tenant={self.tenant_id}).")
        return rotas_df, vendedores_necessarios, len(rotas_df) // vendedores_necessarios

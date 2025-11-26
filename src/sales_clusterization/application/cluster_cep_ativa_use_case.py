#sales_router/src/sales_clusterization/application/cluster_cep_ativa_use_case.py

# ============================================================
# 📦 src/sales_clusterization/application/cluster_cep_ativa_use_case.py
# ============================================================

import pandas as pd
import numpy as np
import uuid
import time
import os
from loguru import logger
from sales_clusterization.domain.haversine_utils import haversine
from sales_clusterization.domain.centers_geolocation_service import CentersGeolocationService



class ClusterCEPAtivaUseCase:
    """
    Clusterização ativa de CEPs com base em endereços de centros informados manualmente.
    - Lê CSV com colunas: Rua_Numero, Bairro, Cidade, UF.
    - Monta o endereço completo e geocodifica os centros.
    - Atribui cada CEP ao centro mais próximo.
    - Calcula distância (km), tempo (min) e outliers.
    - Grava tudo em mkp_cluster_cep com o mesmo padrão do fluxo tradicional.
    """

    def __init__(
        self,
        reader,
        writer,
        tenant_id,
        uf,
        input_id,
        descricao,
        velocidade_media,
        tempo_max_min,
        caminho_centros,
        cidade=None,
        usar_clientes_total=False,
        usar_marketplace=False,
    ):

        self.reader = reader
        self.writer = writer
        self.tenant_id = tenant_id
        self.uf = uf
        self.input_id = input_id
        self.descricao = descricao
        self.velocidade_media = velocidade_media
        self.tempo_max_min = tempo_max_min
        self.cidade = cidade
        self.caminho_centros = caminho_centros
        self.geo_service = CentersGeolocationService(
            reader=self.reader,
            writer=self.writer,
            google_key=os.getenv("GMAPS_API_KEY")
        )

        self.usar_clientes_total = usar_clientes_total
        self.usar_marketplace = usar_marketplace


    # ------------------------------------------------------------
    # Execução principal
    # ------------------------------------------------------------
    def execute(self):
        inicio_execucao = time.time()
        clusterization_id = str(uuid.uuid4())
        logger.info(f"🚀 Iniciando clusterização ativa (tenant={self.tenant_id}, UF={self.uf})")
        logger.info(f"🆔 clusterization_id={clusterization_id}")

        # ============================================================
        # 📥 1. Carrega centros informados
        # ============================================================
        df_centros = pd.read_csv(self.caminho_centros, sep=None, engine="python", encoding="utf-8")
        df_centros.columns = df_centros.columns.str.lower().str.strip()

        # ============================================================
        # 🔧 Normaliza e formata CNPJ para evitar notação científica
        # ============================================================
        if "CNPJ" in df_centros.columns or "cnpj" in df_centros.columns:
            cnpj_col = "CNPJ" if "CNPJ" in df_centros.columns else "cnpj"

            def normalizar_cnpj(valor):
                """Converte e formata CNPJ em notação científica, float ou texto solto."""
                try:
                    s = str(valor).strip().replace(",", ".")
                    # Trata notação científica (ex: 5.75E+13)
                    if "E" in s.upper():
                        s = "{:.0f}".format(float(s))
                    # Extrai apenas dígitos
                    s = "".join(filter(str.isdigit, s))
                    # Garante 14 dígitos
                    s = s.zfill(14)
                    # Formata no padrão oficial
                    if len(s) == 14:
                        return f"{s[:2]}.{s[2:5]}.{s[5:8]}/{s[8:12]}-{s[12:]}"
                    return s
                except Exception:
                    return str(valor).strip()

            df_centros[cnpj_col] = df_centros[cnpj_col].apply(normalizar_cnpj)

        # ============================================================
        # 📋 Validação de colunas obrigatórias
        # ============================================================
        colunas_requeridas = {"rua_numero", "bairro", "cidade", "uf"}

        if not colunas_requeridas.issubset(df_centros.columns):
            raise ValueError(f"❌ O CSV deve conter as colunas: {', '.join(colunas_requeridas)}")

        # ============================================================
        # 🛠️ 1.x – Limpeza de endereço para geocodificação
        # ============================================================
        import re

        def extrair_rua(rua_numero):
            s = str(rua_numero).strip()
            m = re.search(r'\d+', s)
            if m:
                return s[:m.start()].strip().rstrip(",")
            return s

        def extrair_numero_puro(rua_numero):
            s = str(rua_numero).strip()
            m = re.search(r'\d+', s)
            return m.group(0) if m else ""


        # Criar colunas limpas
        df_centros["rua_limpa"] = df_centros["rua_numero"].apply(extrair_rua)
        df_centros["numero_puro"] = df_centros["rua_numero"].apply(extrair_numero_puro)

        # ============================================================
        # 🏗️ Endereço FINAL para geocodificação (limpo)
        # ============================================================
        df_centros["endereco"] = (
            df_centros["rua_limpa"].astype(str).str.strip() + ", "
            + df_centros["numero_puro"].astype(str).str.strip() + ", "
            + df_centros["bairro"].astype(str).str.strip() + ", "
            + df_centros["cidade"].astype(str).str.strip() + " - "
            + df_centros["uf"].astype(str).str.strip() + ", Brasil"
        )

        df_centros.dropna(subset=["endereco"], inplace=True)
        df_centros["cluster_id"] = range(len(df_centros))
        logger.info(f"🏗️ {len(df_centros)} endereços de centros carregados e formatados.")
        
        # ============================================================
        # 🧩 1.1 Inclui informações adicionais (nome e CNPJ se existirem)
        # ============================================================
        
        possiveis_nomes = [c for c in df_centros.columns if "bandeira" in c or "nome" in c]
        df_centros["centro_nome"] = df_centros[possiveis_nomes[0]] if possiveis_nomes else ""
        df_centros["centro_cnpj"] = (
            df_centros["cnpj"] if "cnpj" in df_centros.columns else pd.Series([""] * len(df_centros))
        )


        # ============================================================
        # 🌍 2. Geocodifica centros (com logs detalhados)
        # ============================================================
        latitudes, longitudes, origens = [], [], []

        logger.info("🌍 Iniciando geocodificação dos centros...")

        total_centros = len(df_centros)
        for i, row in df_centros.iterrows():
            endereco = str(row["endereco"]).strip()
            inicio_tempo = time.time()

            logger.info(f"📍 ({i+1}/{total_centros}) Geocodificando: '{endereco}'")

            try:
                lat, lon, origem = self.geo_service.buscar(endereco)
                duracao = round(time.time() - inicio_tempo, 2)

                if lat and lon:
                    logger.success(f"✅ [{origem}] {endereco} → ({lat:.6f}, {lon:.6f}) | {duracao:.2f}s")
                    latitudes.append(lat)
                    longitudes.append(lon)
                    origens.append(origem)
                else:
                    logger.warning(f"⚠️ Falha ao geocodificar '{endereco}' | tempo={duracao:.2f}s")
                    latitudes.append(None)
                    longitudes.append(None)
                    origens.append("falha")

            except Exception as e:
                logger.error(f"❌ Erro inesperado geocodificando '{endereco}': {e}")
                latitudes.append(None)
                longitudes.append(None)
                origens.append("erro")

        logger.info("📊 Estatísticas gerais de geocodificação:")
        logger.info(f"   Total: {total_centros}")
        logger.info(f"   Sucesso: {sum(pd.notna(latitudes))}")
        logger.info(f"   Falhas: {sum(pd.isna(latitudes))}")

        df_centros["lat"] = latitudes
        df_centros["lon"] = longitudes
        df_centros["origem_geo"] = origens
       
        # Salvar centros inválidos ANTES de removê-los
        salvar_centros_invalidos(df_centros, tenant_id=self.tenant_id)

        # Remove centros sem coordenadas válidas
        df_centros = df_centros.dropna(subset=["lat", "lon"]).reset_index(drop=True)

        logger.success(f"✅ Geocodificação de centros concluída: {len(df_centros)} válidos / {total_centros} totais.")
      
        # ============================================================
        # 🏙️ 2.1 Obtém bairro para cada centro (preferencialmente do CSV)
        # ============================================================
        bairros = []
        for i, row in df_centros.iterrows():
            bairro = str(row.get("bairro") or "").strip()
            if not bairro and pd.notna(row.get("lat")) and pd.notna(row.get("lon")):
                try:
                    info_rev = self.geo_service.reverse_geocode(row["lat"], row["lon"])
                    bairro = info_rev.get("bairro", "")
                except Exception:
                    bairro = ""
            bairros.append(bairro)
        df_centros["cluster_bairro"] = bairros

        # ============================================================
        # 📦 3. Carrega base de CEPs do marketplace
        # ============================================================
        registros = self.reader.buscar_ceps(
            usar_marketplace=self.usar_marketplace,
            tenant_id=self.tenant_id,
            input_id=self.input_id,
            uf=self.uf,
            cidade=self.cidade
        )

        if not registros:
            logger.warning("⚠️ Nenhum registro marketplace_cep encontrado.")
            return None

        df_ceps = pd.DataFrame(
            registros,
            columns=["cep", "lat", "lon", "clientes_total", "clientes_target"],
        )
        logger.info(f"📦 {len(df_ceps)} CEPs carregados para atribuição.")

        # ============================================================
        # ⚖️ 3.1 Define o campo de peso (clientes_target padrão)
        # ============================================================
        df_ceps["peso"] = (
            df_ceps["clientes_total"] if self.usar_clientes_total else df_ceps["clientes_target"]
        )
        logger.info(
            f"⚙️ Peso definido como {'clientes_total' if self.usar_clientes_total else 'clientes_target'}"
        )

        # ============================================================
        # 🧹 3.2 Remove CEPs irrelevantes (sem peso)
        # ============================================================
        if not self.usar_clientes_total:
            antes = len(df_ceps)
            df_ceps = df_ceps[df_ceps["clientes_target"] > 0].copy()
            removidos = antes - len(df_ceps)
            if removidos > 0:
                logger.warning(f"🧹 Removidos {removidos} CEPs com clientes_target = 0 (sem relevância).")


        # ============================================================
        # 🧮 4. Atribui cada CEP ao centro mais próximo
        # ============================================================
        coords_centros = df_centros[["lat", "lon"]].values
        coords_ceps = df_ceps[["lat", "lon"]].values
        logger.info("🧭 Calculando distâncias Haversine...")

        dist_matrix = np.zeros((len(coords_ceps), len(coords_centros)))
        for i, (lat1, lon1) in enumerate(coords_ceps):
            dist_matrix[i, :] = [
                haversine((lat1, lon1), (lat2, lon2)) for lat2, lon2 in coords_centros
            ]

        idx_min = np.argmin(dist_matrix, axis=1)
        df_ceps["cluster_id"] = idx_min
        df_ceps = df_ceps.merge(
            df_centros[["cluster_id", "centro_nome", "centro_cnpj"]],
            on="cluster_id", how="left"
        )
        df_ceps["centro_nome"] = df_ceps["centro_nome"].fillna("").astype(str)
        df_ceps["centro_cnpj"] = df_ceps["centro_cnpj"].fillna("").astype(str)

        df_ceps["distancia_km"] = dist_matrix[np.arange(len(coords_ceps)), idx_min]
        df_ceps["tempo_min"] = (df_ceps["distancia_km"] / self.velocidade_media) * 60

        # ============================================================
        # 🧹 REMOVER OUTLIERS ANTES DO RESTO DO PROCESSAMENTO
        # ============================================================
        OUTLIER_MAX_KM = 30  # você pode ajustar (SP ideal = 20–30 km)

        antes = len(df_ceps)
        df_ceps["is_outlier"] = (
            (df_ceps["tempo_min"] > self.tempo_max_min) |
            (df_ceps["distancia_km"] > OUTLIER_MAX_KM)
        )

        df_ceps = df_ceps[~df_ceps["is_outlier"]].copy()
        removidos = antes - len(df_ceps)

        logger.warning(f"🧹 Outliers removidos no início: {removidos} (>{OUTLIER_MAX_KM} km ou tempo > {self.tempo_max_min} min)")
        logger.info("✅ Atribuição de CEPs concluída (após limpeza).")


        # ============================================================
        # 🧭 5. Associa coordenadas do centro
        # ============================================================
        df_ceps["cluster_lat"] = df_ceps["cluster_id"].apply(
            lambda x: df_centros.loc[x, "lat"]
        )
        df_ceps["cluster_lon"] = df_ceps["cluster_id"].apply(
            lambda x: df_centros.loc[x, "lon"]
        )

        # ============================================================
        # 🏙️ 5.1 Inclui bairro do centro no DataFrame de CEPs
        # ============================================================
        if "cluster_bairro" in df_centros.columns:
            df_ceps = df_ceps.merge(
                df_centros[["cluster_id", "cluster_bairro"]],
                on="cluster_id",
                how="left"
            )
        else:
            df_ceps["cluster_bairro"] = ""


        # ============================================================
        # 💾 6. Persiste resultados
        # ============================================================
        lista_clusters = []
        for _, row in df_ceps.iterrows():
            lista_clusters.append(
                {
                    "tenant_id": self.tenant_id,
                    "input_id": self.input_id,
                    "clusterization_id": clusterization_id,
                    "uf": self.uf,
                    "cep": row["cep"],
                    "cluster_id": int(row["cluster_id"]),
                    "centro_nome": str(row.get("centro_nome", "")),
                    "centro_cnpj": str(row.get("centro_cnpj", "")),
                    "cluster_bairro": str(row.get("cluster_bairro", "")),  # 🆕 adiciona bairro do centro
                    "clientes_total": int(row["clientes_total"] or 0),
                    "clientes_target": int(row["clientes_target"] or 0),
                    "lat": float(row["lat"]),
                    "lon": float(row["lon"]),
                    "cluster_lat": float(row["cluster_lat"]),
                    "cluster_lon": float(row["cluster_lon"]),
                    "distancia_km": float(row["distancia_km"]),
                    "tempo_min": float(row["tempo_min"]),
                    "is_outlier": bool(row["is_outlier"]),
                    "modo_clusterizacao": "ativa",
                    "cluster_bairro": str(row.get("cluster_bairro", "")),
                }
            )

        inseridos = self.writer.inserir_mkp_cluster_cep(lista_clusters)
        total_clusters = df_centros.shape[0]
        total_ceps = len(df_ceps)
        total_outliers = df_ceps["is_outlier"].sum()

        logger.success(
            f"💾 {inseridos} registros gravados em mkp_cluster_cep "
            f"| clusters={total_clusters} | ceps={total_ceps} | outliers={total_outliers}"
        )

        # ============================================================
        # 📈 6.1. Estatísticas por cluster (resumo operacional)
        # ============================================================
        logger.info("📊 Gerando resumo operacional por cluster:")
        resumo = []
        for cid, grupo in df_ceps.groupby("cluster_id"):
            tempo_medio = grupo["tempo_min"].mean()
            tempo_max = grupo["tempo_min"].max()
            dist_media = grupo["distancia_km"].mean()
            dist_max = grupo["distancia_km"].max()
            outliers = grupo["is_outlier"].sum()
            total = len(grupo)

            logger.info(
                f"   🧩 Cluster {cid:02d}: {total} CEPs | "
                f"Dist média={dist_media:.2f} km | máx={dist_max:.2f} km | "
                f"Tempo médio={tempo_medio:.1f} min | máx={tempo_max:.1f} min | "
                f"Outliers={outliers}"
            )

            resumo.append({
                "cluster_id": int(cid),
                "total_ceps": total,
                "distancia_media_km": round(dist_media, 2),
                "distancia_max_km": round(dist_max, 2),
                "tempo_medio_min": round(tempo_medio, 1),
                "tempo_max_min": round(tempo_max, 1),
                "outliers": int(outliers),
            })

        # ============================================================
        # 📊 7. Gera resumo automático (CSV)
        # ============================================================
        try:
            from sales_clusterization.reporting.export_resumo_clusters_cep import (
                exportar_resumo_clusters,
            )
            logger.info("📈 Gerando resumo de clusters (CSV)...")
            exportar_resumo_clusters(self.tenant_id, clusterization_id)
        except Exception as e:
            logger.warning(f"⚠️ Falha ao gerar resumo automático: {e}")

        duracao = round(time.time() - inicio_execucao, 2)

        logger.success(f"🏁 Clusterização ativa finalizada com sucesso | clusterization_id={clusterization_id}")
        return {
            "status": "done",
            "tenant_id": self.tenant_id,
            "input_id": self.input_id,
            "descricao": self.descricao,
            "uf": self.uf,
            "clusterization_id": clusterization_id,
            "total_clusters": total_clusters,
            "total_ceps": total_ceps,
            "total_outliers": int(total_outliers),
            "duracao_segundos": duracao,
            "resumo_operacional": resumo,
        }

# ============================================================
# 📄 Função auxiliar: salvar centros inválidos
# ============================================================
from datetime import datetime
import os
from pdv_preprocessing.domain.utils_geo import coordenada_generica

def salvar_centros_invalidos(df_centros, tenant_id):
        df_invalidos = df_centros[
            (df_centros["lat"].isna()) |
            (df_centros["lon"].isna()) |
            df_centros.apply(lambda r: coordenada_generica(r["lat"], r["lon"]), axis=1)
        ].copy()

        if df_invalidos.empty:
            logger.info("🟢 Nenhum centro inválido para salvar.")
            return

        pasta = f"output/erros_geocodificacao/{tenant_id}"
        os.makedirs(pasta, exist_ok=True)

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        caminho_csv = f"{pasta}/centros_invalidos_{timestamp}.csv"

        df_invalidos.to_csv(caminho_csv, index=False, sep=";")
        logger.warning(f"⚠️ CSV gerado com centros inválidos: {caminho_csv}")


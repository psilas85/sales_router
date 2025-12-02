# sales_router/src/sales_clusterization/mkp_pdv/application/cluster_pdv_ativa_use_case.py

# sales_router/src/sales_clusterization/mkp_pdv/application/cluster_pdv_ativa_use_case.py

import uuid
import pandas as pd
import re
from loguru import logger

from ..infrastructure.database_reader import MKPPDVReader
from ..infrastructure.database_writer import MKPPDVWriter
from ..domain.pdv_assignment import PDVAssignment
from ..domain.centers_loader import CentersLoader


class ClusterPDVAtivaUseCase:
    """
    Clusterização MKP por PDV — modo ATIVA.
    """

    # ------------------------------------------------------------
    # Funções auxiliares
    # ------------------------------------------------------------
    def _extrair_rua(self, rua_numero):
        s = str(rua_numero).strip()
        m = re.search(r"\d+", s)
        if m:
            return s[:m.start()].strip().rstrip(",")
        return s

    def _extrair_numero_puro(self, rua_numero):
        s = str(rua_numero).strip()
        m = re.search(r"\d+", s)
        return m.group(0) if m else ""

    # ------------------------------------------------------------
    # Carregar centros do CSV e preparar campos
    # ------------------------------------------------------------
    def _carregar_centros_csv(self, caminho_csv):
        df = pd.read_csv(caminho_csv, sep=";", encoding="utf-8")
        df.columns = df.columns.str.lower().str.strip()

        obrigatorias = {
            "bandeira cliente", "cidade", "rua_numero",
            "cnpj", "razão", "uf", "bairro"
        }
        faltando = obrigatorias - set(df.columns)
        if faltando:
            raise ValueError(f"CSV inválido. Faltam colunas: {faltando}")

        df["bandeira"] = df["bandeira cliente"]
        df["cliente"] = df["razão"]
        df["cnpj"] = df["cnpj"].astype(str)

        df["rua_limpa"] = df["rua_numero"].apply(self._extrair_rua)
        df["numero_puro"] = df["rua_numero"].apply(self._extrair_numero_puro)

        df["endereco_fmt"] = (
            df["rua_limpa"] + ", " +
            df["numero_puro"] + ", " +
            df["bairro"] + ", " +
            df["cidade"] + " - " +
            df["uf"] + ", Brasil"
        )

        df["cluster_id"] = df.index + 1

        return df

    # ------------------------------------------------------------
    # Execução principal
    # ------------------------------------------------------------
    def executar(self, tenant_id, uf, cidade, input_id, centros_csv, descricao):
        logger.info("🚀 Iniciando clusterização MKP PDV (ativa)...")

        # 1) Carregar e preparar CSV
        df_centros = self._carregar_centros_csv(centros_csv)
        logger.info(f"📥 Centros carregados: {len(df_centros)}")

        # 2) Geocodificar centros
        centros = CentersLoader.geocodificar_centros(df_centros)
        logger.info(f"📌 Centros geocodificados: {len(centros)}")

        if not centros:
            raise RuntimeError("Nenhum centro foi geocodificado.")

        # 3) Carregar PDVs
        pdvs = MKPPDVReader.carregar_pdvs(tenant_id, uf, cidade, input_id)
        if not pdvs:
            raise RuntimeError("Nenhum PDV encontrado.")
        logger.info(f"📦 {len(pdvs)} PDVs carregados.")

        # 4) Atribuir PDVs aos centros
        lista_atribuida = PDVAssignment.atribuir(pdvs, centros)
        logger.info("🧭 Atribuição concluída.")

        # 5) Persistência
        clusterization_id = str(uuid.uuid4())

        # 5.1 - Persistir centros
        MKPPDVWriter.inserir_centros(
            centros,
            tenant_id,
            input_id,
            clusterization_id
        )

        # 5.2 - Persistir PDVs
        MKPPDVWriter.inserir_pdv_clusters(
            lista_atribuida,
            tenant_id,
            input_id,
            clusterization_id,
            modo="ativa"
        )

        logger.success(f"🏁 Clusterização ATIVA finalizada | ID={clusterization_id}")
        return clusterization_id

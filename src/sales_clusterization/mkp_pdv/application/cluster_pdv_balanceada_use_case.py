# sales_router/src/sales_clusterization/mkp_pdv/application/cluster_pdv_balanceada_use_case.py

# sales_router/src/sales_clusterization/mkp_pdv/application/cluster_pdv_balanceada_use_case.py

import uuid
from loguru import logger

from .cluster_pdv_ativa_use_case import ClusterPDVAtivaUseCase
from ..domain.pdv_balanceamento import PDVBalanceamento
from ..infrastructure.database_reader import MKPPDVClusterReader
from ..infrastructure.database_writer import MKPPDVWriter


class ClusterPDVBalanceadaUseCase:

    def executar(self, tenant_id, uf, cidade, input_id, centros_csv, descricao, min_pdv, max_pdv):

        # 1) Executa clusterização ativa
        ativa = ClusterPDVAtivaUseCase()
        ativa_clusterization_id = ativa.executar(
            tenant_id, uf, cidade, input_id, centros_csv, descricao
        )

        logger.info("⚖️ Iniciando balanceamento PDV...")

        # 2) Carrega os centros usados na clusterização ativa
        centros_ativos = MKPPDVClusterReader.carregar_centros_da_clusterizacao(
            tenant_id, ativa_clusterization_id
        )

        if not centros_ativos:
            raise RuntimeError("Nenhum centro encontrado na clusterização ativa.")

        # 3) Carrega PDVs já clusterizados (ativa)
        lista = MKPPDVClusterReader.carregar_pdvs_da_clusterizacao(
            tenant_id, ativa_clusterization_id
        )

        # Lista precisa ter "bairro" para o balanceamento
        for item in lista:
            item["bairro"] = item.get("cluster_bairro")

        # 4) Balanceamento min/max
        balanceado = PDVBalanceamento.balancear(
            lista, min_pdv, max_pdv, centros_ativos
        )

        # 5) Novo ID
        clusterization_id = str(uuid.uuid4())

        # 6) Salvar PDVs balanceados
        MKPPDVWriter.inserir_pdv_clusters(
            balanceado,
            tenant_id,
            input_id,
            clusterization_id,
            modo="balanceada",
        )

        # 7) Persistir centros balanceados
        centros_para_inserir = []
        for c in centros_ativos:
            centros_para_inserir.append({
                "cluster_id": c["cluster_id"],
                "bandeira": c.get("bandeira"),
                "cliente": c.get("cliente"),
                "cnpj": c.get("cnpj"),
                "lat": c["lat"],
                "lon": c["lon"],
                "endereco": c.get("endereco"),
                "bairro": c.get("bairro"),  # ← ESSENCIAL
            })

        MKPPDVWriter.inserir_centros(
            centros_para_inserir,
            tenant_id,
            input_id,
            clusterization_id
        )

        logger.success(
            f"🏁 Clusterização PDV Balanceada finalizada | id={clusterization_id}"
        )

        return clusterization_id

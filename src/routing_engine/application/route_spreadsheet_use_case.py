# sales_router/src/routing_engine/application/route_spreadsheet_use_case.py

from __future__ import annotations

import time
import uuid
import os
from typing import List, Dict
import math

import pandas as pd
from loguru import logger

from routing_engine.routing_task_parallel import executar_routing_job

from routing_engine.services.spreadsheet_loader_service import SpreadsheetLoaderService
from routing_engine.domain.spreadsheet_validator import SpreadsheetValidator
from routing_engine.domain.entities import PDVData, RouteGroup

from routing_engine.application.balanced_subcluster_splitter import dividir_grupo_em_rotas_balanceadas
from routing_engine.application.route_optimizer import RouteOptimizer
from routing_engine.application.route_distance_service import RouteDistanceService
from routing_engine.application.consultor_service import ConsultorService

from routing_engine.infrastructure.routing_history_repository import RoutingHistoryRepository
from routing_engine.utils.excel_exporter import ExcelExporter


class RouteSpreadsheetUseCase:

    def __init__(self):

        self.loader = SpreadsheetLoaderService()
        self.validator = None

        
        self.history_repo = RoutingHistoryRepository()
        self.exporter = ExcelExporter()

    def _processar_grupo(
        self,
        grupo_id,
        lista_pdvs,
        consultor_service,
        dias_uteis,
        freq_visita,
        aplicar_two_opt,
        min_pdvs_rota,
        max_pdvs_rota
    ):
        fonte = lista_pdvs[0].fonte_grupo

        if fonte != "consultor":
            raise ValueError("Modo atual exige agrupamento por consultor")

        consultor = str(grupo_id).strip().upper()

        base_lat, base_lon = consultor_service.get_base(consultor)

        route_group = RouteGroup(
            group_id=grupo_id,
            group_type=fonte,
            centro_lat=base_lat,
            centro_lon=base_lon,
            n_pdvs=len(lista_pdvs),
            pdvs=lista_pdvs
        )

        return dividir_grupo_em_rotas_balanceadas(
            route_group=route_group,
            dias_uteis=dias_uteis,
            freq_padrao=freq_visita,
            route_optimizer=self.route_optimizer,
            aplicar_two_opt=aplicar_two_opt,
            min_pdvs_rota=min_pdvs_rota,
            max_pdvs_rota=max_pdvs_rota
        )

    def execute(
        self,
        file_bytes: bytes,
        filename: str,
        tenant_id: int = 1,
        origem: str = "upload",
        dias_uteis: int = 21,
        freq_visita: float = 1.0,
        min_pdvs_rota: int = 8,
        max_pdvs_rota: int = 12,
        aplicar_two_opt: bool = True,
        output_dir: str = "/app/data/outputs"
    ) -> dict:

        start = time.time()
        request_id = str(uuid.uuid4())

        logger.info(f"[ROUTING_START] request_id={request_id}")

        # =========================================================
        # LOAD + VALIDATE (CENTRALIZADO)
        # =========================================================
        df_raw = self.loader.load(file_bytes, filename)

        self.validator = SpreadsheetValidator(tenant_id)
        validation = self.validator.validate(df_raw)

        df = validation.dataframe
        df_invalidos = pd.DataFrame()  # validator já bloqueia erro

        logger.info(f"📥 Linhas válidas: {len(df)}")

        # =========================================================
        # BUILD PDVS
        # =========================================================
        pdvs: List[PDVData] = []

        for idx, row in df.iterrows():
            pdvs.append(
                PDVData(
                    pdv_id=idx,
                    cnpj=row["cnpj"],
                    nome_fantasia=row.get("nome_fantasia"),
                    logradouro=row["logradouro"],
                    numero=row.get("numero"),
                    bairro=row.get("bairro"),
                    cidade=row["cidade"],
                    uf=row["uf"],
                    cep=row.get("cep"),
                    grupo_utilizado=row["grupo_utilizado"],
                    fonte_grupo=row["fonte_grupo"],
                    lat=float(row["lat"]),
                    lon=float(row["lon"]),
                    freq_visita=freq_visita
                )
            )

        # =========================================================
        # AGRUPAMENTO
        # =========================================================
        grupos_dict: Dict[str, List[PDVData]] = {}

        for p in pdvs:
            grupos_dict.setdefault(p.grupo_utilizado, []).append(p)

        total_grupos = len(grupos_dict)

        # =========================================================
        # EXECUÇÃO PARALELA
        # =========================================================
        resultados, stats, falhados = executar_routing_job(grupos_dict, {
            "tenant_id": tenant_id,
            "dias_uteis": dias_uteis,
            "freq_visita": freq_visita,
            "min_pdvs_rota": min_pdvs_rota,
            "max_pdvs_rota": max_pdvs_rota,
            "aplicar_two_opt": aplicar_two_opt,
            "v_kmh": 60.0,
            "service_min": 30.0,
            "alpha_path": 1.3
        })

        stats = stats or {}

        cache_hits = stats.get("cache_hits", 0)
        osrm_hits = stats.get("osrm_hits", 0)
        google_hits = stats.get("google_hits", 0)
        haversine_hits = stats.get("haversine_hits", 0)

        # =========================================================
        # VALIDAÇÃO DE INTEGRIDADE
        # =========================================================
        logger.error(f"Resultados recebidos: {len(resultados)} / {total_grupos}")

        if len(resultados) != total_grupos:
            logger.warning(
                f"⚠️ Grupos com erro: {total_grupos - len(resultados)} de {total_grupos}"
            )

        resultados.sort(key=lambda x: str(x.get("grupo_utilizado") or x.get("group_id") or ""))

        # =========================================================
        # DATAFRAMES
        # =========================================================
        df_detalhe = []
        df_resumo = []

        rota_global_id = 1

        for grupo in resultados:

            if not grupo.get("subclusters"):
                continue

            for sub in grupo["subclusters"]:

                rota_id = f"R{rota_global_id}"

                df_resumo.append({
                    "rota_id": rota_id,
                    "grupo_utilizado": grupo.get("grupo_utilizado"),
                    "fonte_grupo": "consultor",
                    "qtd_pdvs": sub["n_pdvs"],
                    "distancia_km": sub["dist_total_km"],
                    "tempo_min": sub["tempo_total_min"],
                    "request_id": request_id
                })

                for seq, p in enumerate(sub["pdvs"], start=1):

                    df_detalhe.append({
                        "cnpj": p["cnpj"],
                        "nome_fantasia": p.get("nome_fantasia"),
                        "cidade": p.get("cidade"),
                        "uf": p.get("uf"),
                        "grupo_utilizado": p.get("grupo_utilizado"),
                        "rota_id": rota_id,
                        "sequencia": seq,
                        "lat": p.get("lat"),
                        "lon": p.get("lon"),
                        "request_id": request_id
                    })

                rota_global_id += 1

        df_detalhe = pd.DataFrame(df_detalhe)
        df_resumo = pd.DataFrame(df_resumo)

        # =========================================================
        # EXPORT
        # =========================================================
        df_metricas = pd.DataFrame([{
            "total_pdvs_validos": len(pdvs),
            "total_pdvs_invalidos": len(df_invalidos),
            "total_grupos_validos": total_grupos,
            "total_rotas": rota_global_id - 1,
            "cache_hits": cache_hits,
            "osrm_hits": osrm_hits,
            "google_hits": google_hits,
            "haversine_hits": haversine_hits,
            "tempo_execucao_ms": int((time.time() - start) * 1000)
        }])

        filename_out = f"routing_{request_id}.xlsx"

        file_path = self.exporter.export(
            df_detalhe=df_detalhe,
            df_resumo=df_resumo,
            df_metricas=df_metricas,
            df_invalidos=pd.DataFrame(),
            output_dir=output_dir,
            filename=filename_out
        )

        # =========================================================
        # HISTÓRICO
        # =========================================================
        self.history_repo.salvar_historico(
            request_id=request_id,
            tenant_id=tenant_id,
            origem=origem,
            total_pdvs=len(pdvs),
            total_grupos=total_grupos,
            total_rotas=rota_global_id - 1,
            cache_hits=cache_hits,
            osrm_hits=osrm_hits,
            google_hits=google_hits,
            haversine_hits=haversine_hits,
            tempo_execucao_ms=int((time.time() - start) * 1000)
        )

        logger.success(f"[ROUTING_DONE] request_id={request_id} file={file_path}")

        # =========================================================
        # BUILD ROTAS
        # =========================================================
        rotas = []

        rota_global_id = 1

        for grupo in resultados:

            if not grupo.get("subclusters"):
                continue

            for sub in grupo["subclusters"]:

                rota_id = f"R{rota_global_id}"

                rota_coord = sub.get("rota_coord", [])

                if not rota_coord:
                    rota_global_id += 1
                    continue

                rotas.append({
                    "rota_id": rota_id,
                    "cluster": grupo.get("grupo_utilizado"),
                    "veiculo": None,
                    "rota_coord": rota_coord
                })

                rota_global_id += 1

        # =========================================================
        # RETURN FINAL
        # =========================================================
        total_grupos_processados = len(resultados)
        total_grupos_com_erro = len(falhados) if falhados else 0

        return {
            "output": file_path,
            "rotas": rotas,
            "erros": list(falhados.values()) if falhados else [],
            "invalidos": [],
            "metricas": {
                "total_pdvs_validos": len(pdvs),
                "total_pdvs_invalidos": 0,
                "total_grupos_validos": total_grupos,
                "total_grupos_enviados": total_grupos,
                "grupos_processados": total_grupos_processados,
                "grupos_com_erro": total_grupos_com_erro,
                "total_rotas": rota_global_id - 1,
                "taxa_sucesso": round(
                    (total_grupos_processados / total_grupos) * 100, 1
                ) if total_grupos > 0 else 0,
                "cache_hits": cache_hits,
                "osrm_hits": osrm_hits,
                "google_hits": google_hits,
                "haversine_hits": haversine_hits,
                "tempo_execucao_ms": int((time.time() - start) * 1000),
                "validacao": {
                    "qtd_invalidos": 0,
                    "detalhes": []
                }
            }
        }
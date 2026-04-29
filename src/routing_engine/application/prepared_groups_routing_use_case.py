from __future__ import annotations

from typing import Any

from rq import get_current_job

from routing_engine.routing_task_parallel import executar_routing_job


class PreparedGroupsRoutingUseCase:
    def execute(
        self,
        groups: list[dict[str, Any]],
        *,
        tenant_id: int,
        dias_uteis: int,
        freq_visita: float,
        min_pdvs_rota: int,
        max_pdvs_rota: int,
        aplicar_two_opt: bool,
        modo_calculo: str = "frequencia",
        preserve_sequence: bool = False,
        v_kmh: float = 60.0,
        service_min: float = 30.0,
        alpha_path: float = 1.3,
    ):
        return executar_routing_job(
            groups,
            {
                "tenant_id": tenant_id,
                "dias_uteis": dias_uteis,
                "freq_visita": freq_visita,
                "min_pdvs_rota": min_pdvs_rota,
                "max_pdvs_rota": max_pdvs_rota,
                "aplicar_two_opt": aplicar_two_opt,
                "modo_calculo": modo_calculo,
                "preserve_sequence": preserve_sequence,
                "v_kmh": v_kmh,
                "service_min": service_min,
                "alpha_path": alpha_path,
            },
        )


def executar_prepared_groups_master_job(payload: dict[str, Any]) -> dict[str, Any]:
    job = get_current_job()

    if job:
        job.meta.update({
            "progress": 0,
            "step": "Preparando grupos recebidos",
        })
        job.save_meta()

    resultados, stats, falhados = PreparedGroupsRoutingUseCase().execute(
        payload["groups"],
        tenant_id=payload["tenant_id"],
        dias_uteis=payload["params"]["dias_uteis"],
        freq_visita=payload["params"]["frequencia_visita"],
        min_pdvs_rota=payload["params"]["min_pdvs_rota"],
        max_pdvs_rota=payload["params"]["max_pdvs_rota"],
        aplicar_two_opt=payload["params"].get("twoopt", False),
        modo_calculo=payload["params"].get("modo_calculo", "frequencia"),
        preserve_sequence=payload["params"].get("preserve_sequence", False),
        v_kmh=payload["params"].get("v_kmh", 60.0),
        service_min=payload["params"].get("service_min", 30.0),
        alpha_path=payload["params"].get("alpha_path", 1.3),
    )

    total_groups = len(payload["groups"])
    processed_groups = len(resultados)
    failed_groups = len(falhados)

    status = "done"
    if failed_groups:
        status = "done_with_warnings" if processed_groups > 0 else "partial_failed"

    result = {
        "routing_id": payload["routing_id"],
        "tenant_id": payload["tenant_id"],
        "metrics": {
            "groups_received": total_groups,
            "groups_processed": processed_groups,
            "groups_failed": failed_groups,
            **(stats or {}),
        },
        "results": resultados,
        "failures": list(falhados.values()) if falhados else [],
    }

    if job:
        job.meta.update({
            "progress": 100,
            "step": "Execução concluída",
            "summary": result["metrics"],
        })
        job.save_meta()

    return {
        "status": status,
        "result": result,
    }
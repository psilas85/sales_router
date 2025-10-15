#sales_router/src/sales_clusterization/jobs.py

import os
import re
import json
import subprocess
from datetime import datetime
from rq import get_current_job
from loguru import logger
from src.database.pipeline_history_service import registrar_historico_pipeline


# ============================================================
# 🚀 Função: processar clusterização (pipeline assíncrono)
# ============================================================
def processar_clusterizacao(job_id, tenant_id, uf, cidade, algo, k_forcado=None, modo_forcar=False):
    """
    Executa o processo de clusterização de PDVs de forma assíncrona (via RQ).
    - Cria um subprocesso para rodar o módulo principal da clusterização.
    - Registra o progresso no histórico do pipeline.
    """
    job = get_current_job()
    etapa = "clusterization"

    logger.info(f"🚀 Iniciando job de clusterização ({job_id}) | tenant={tenant_id} | {uf}-{cidade} | algo={algo}")

    # 1️⃣ Atualiza histórico inicial
    registrar_historico_pipeline(
        tenant_id=tenant_id,
        job_id=job_id,                     # ✅ Adicionado
        etapa=etapa,
        status="running",
        mensagem=f"Iniciando clusterização ({uf}-{cidade})",
    )

    try:
        # ============================================================
        # 🔧 Monta comando CLI
        # ============================================================
        comando = [
            "python3",
            "-m",
            "src.sales_clusterization.cli.run_cluster",
            "--tenant_id", str(tenant_id),
            "--uf", uf,
            "--cidade", cidade,
            "--algo", algo,
        ]

        if k_forcado:
            comando += ["--k", str(k_forcado)]
        if modo_forcar:
            comando.append("--modo_forcar")

        logger.info(f"▶️ Executando comando: {' '.join(comando)}")

        # ============================================================
        # 🔄 Executa subprocesso com PYTHONPATH corrigido
        # ============================================================
        process = subprocess.Popen(
            comando,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="ignore",
            env={**os.environ, "PYTHONPATH": "/app/src"},  # ✅ Corrige path
        )

        linhas = []
        for line in iter(process.stdout.readline, ""):
            line = line.strip()
            if line:
                logger.info(f"[{job_id}] {line}")
                linhas.append(line)

        process.wait()

        if process.returncode != 0:
            logger.error(f"❌ Clusterização falhou com código {process.returncode}")
            raise Exception("\n".join(linhas))

        # ============================================================
        # 🧠 Interpreta resultado
        # ============================================================
        resumo = None
        for line in linhas:
            if line.startswith("{") and line.endswith("}"):
                try:
                    resumo = json.loads(line)
                    break
                except Exception:
                    pass

        if resumo:
            msg = f"✅ Clusterização concluída com sucesso | run_id={resumo.get('run_id')} | K={resumo.get('k_final')}"
            logger.success(msg)
        else:
            msg = "✅ Clusterização concluída (sem resumo JSON detectado)."
            logger.success(msg)

        # ============================================================
        # 💾 Atualiza histórico final
        # ============================================================
        registrar_historico_pipeline(
            tenant_id=tenant_id,
            job_id=job_id,                # ✅ Adicionado
            etapa=etapa,
            status="done",
            mensagem=msg,
            metadata={"resultado": resumo or {}},
        )

        return {
            "status": "done",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "uf": uf,
            "cidade": cidade,
            "algo": algo,
            "resultado": resumo or {},
        }

    except Exception as e:
        logger.error(f"❌ Erro no job {job_id}: {e}", exc_info=True)
        registrar_historico_pipeline(
            tenant_id=tenant_id,
            job_id=job_id,               # ✅ Adicionado
            etapa=etapa,
            status="error",
            mensagem=str(e),
        )
        return {
            "status": "error",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "erro": str(e),
        }

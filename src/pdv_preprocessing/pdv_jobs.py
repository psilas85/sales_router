#sales_router/src/pdv_preprocessing/pdv_jobs.py

# ============================================================
# 📦 src/pdv_preprocessing/pdv_jobs.py — WORKER FINAL (COM WRITE EM HISTÓRICO)
# ============================================================

import logging
import subprocess
import json
from uuid import uuid4, UUID
from rq import get_current_job
import sys
import re

from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter

logger = logging.getLogger("pdv_jobs")
logger.setLevel(logging.INFO)

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]


# ============================================================
# 🧼 Normalização forte de UUID (remove tabs, newlines, espaços invisíveis)
# ============================================================
_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

def normalizar_uuid(valor: str | None) -> str | None:
    """
    - Remove qualquer whitespace (inclui \t, \n, \r, NBSP)
    - Remove qualquer caractere que não seja [0-9a-fA-F-]
    - Valida e retorna string canônica UUID
    """
    if valor is None:
        return None

    if not isinstance(valor, str):
        valor = str(valor)

    bruto = valor

    # remove whitespace (tabs/newlines/espaços invisíveis)
    valor = re.sub(r"\s+", "", valor)

    # mantém só hex e hífen
    valor = re.sub(r"[^0-9a-fA-F-]", "", valor)

    if not valor:
        return None

    # valida
    try:
        uid = str(UUID(valor))
    except Exception:
        return None

    # garante formato padrão
    if not _UUID_RE.match(uid):
        return None

    if bruto != uid:
        logger.warning(f"⚠️ input_id normalizado: bruto='{bruto}' -> uuid='{uid}'")

    return uid


# ============================================================
# 🚀 Função principal do worker
# ============================================================
def processar_csv(tenant_id, file_path, descricao):
    job = get_current_job()
    writer = DatabaseWriter()

    # --------------------------------------------------------
    # Job ID garantido
    # --------------------------------------------------------
    if job:
        try:
            job_id = str(UUID(job.id))
        except Exception:
            job_id = str(uuid4())
    else:
        job_id = str(uuid4())

    logger.info(f"🚀 Iniciando job {job_id} para tenant {tenant_id}")

    try:
        comando = [
            "python3",
            "-u",
            "src/pdv_preprocessing/main_pdv_preprocessing.py",
            "--tenant", str(tenant_id),
            "--arquivo", file_path,
            "--descricao", descricao,
        ]

        # Progresso inicial
        if job:
            job.meta.update({"step": "Iniciando", "progress": 0})
            job.save_meta()

        logger.info(f"▶️ Executando: {' '.join(comando)}")

        proc = subprocess.Popen(
            comando,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True,
        )

        resumo = {}
        json_line = None

        # ----------------------------------------------------
        # Streaming + progresso
        # ----------------------------------------------------
        for line in proc.stdout:
            line = line.rstrip("\n")
            logger.info(f"[MAIN] {line}")

            try:
                obj = json.loads(line)

                # Progresso
                if isinstance(obj, dict) and obj.get("event") == "progress":
                    pct = obj.get("pct", 0)
                    step = obj.get("step", "")

                    if job:
                        job.meta.update({"progress": pct, "step": step})
                        job.save_meta()
                    continue

                # JSON final
                if isinstance(obj, dict) and "status" in obj:
                    resumo = obj
                    json_line = line
                    continue

            except json.JSONDecodeError:
                pass

        proc.wait()

        if proc.returncode != 0:
            raise RuntimeError(
                f"main_pdv_preprocessing retornou código {proc.returncode}"
            )

        if not resumo and json_line:
            resumo = json.loads(json_line)

        if not resumo:
            raise RuntimeError(
                "Não foi possível capturar o JSON final do main_pdv_preprocessing"
            )

        # ----------------------------------------------------
        # ✅ FIX CRÍTICO: normaliza input_id vindo do main
        # ----------------------------------------------------
        input_id_norm = normalizar_uuid(resumo.get("input_id"))

        logger.info(
            f"✅ Job {job_id}: "
            f"{resumo.get('validos', 0)} válidos / "
            f"{resumo.get('invalidos', 0)} inválidos | "
            f"input_id={input_id_norm or 'NULL'}"
        )

        # ----------------------------------------------------
        # ✅ SALVA HISTÓRICO NO BANCO (PONTO CRÍTICO)
        # ----------------------------------------------------
        writer.salvar_historico_pdv_job(
            tenant_id=tenant_id,
            job_id=job_id,
            arquivo=resumo.get("arquivo"),
            status=resumo.get("status"),
            total_processados=resumo.get("total_processados", 0),
            validos=resumo.get("validos", 0),
            invalidos=resumo.get("invalidos", 0),
            inseridos=resumo.get("inseridos", 0),
            sobrescritos=resumo.get("sobrescritos", 0),
            arquivo_invalidos=resumo.get("arquivo_invalidos"),
            mensagem=resumo.get("mensagem"),
            descricao=descricao,
            input_id=input_id_norm,  # <- SOMENTE UUID LIMPO
        )

        if job:
            job.meta.update({"step": "Finalizado", "progress": 100})
            job.save_meta()

        # Retorno leve (frontend busca detalhes no banco)
        return {
            "status": resumo.get("status"),
            "job_id": job_id,
            "tenant_id": tenant_id,
            "input_id": input_id_norm,  # <- devolve limpo
        }

    # ========================================================
    # ❌ ERRO
    # ========================================================
    except Exception as e:
        logger.error(f"💥 Erro no job {job_id}: {e}", exc_info=True)

        # Salva erro no histórico
        try:
            writer.salvar_historico_pdv_job(
                tenant_id=tenant_id,
                job_id=job_id,
                arquivo=file_path,
                status="error",
                mensagem=str(e),
                descricao=descricao,
            )
        except Exception:
            pass

        if job:
            job.meta.update({"step": "Erro", "progress": 100})
            job.save_meta()

        return {
            "status": "error",
            "job_id": job_id,
            "tenant_id": tenant_id,
            "error": str(e),
        }

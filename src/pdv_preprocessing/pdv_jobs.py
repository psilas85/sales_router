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

# ============================================================
# 🔧 Logger
# ============================================================

logger = logging.getLogger("pdv_jobs")
logger.setLevel(logging.INFO)
logger.propagate = False

handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
formatter = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.handlers = [handler]


# ============================================================
# 🧼 Normalização forte de UUID
# ============================================================

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{12}$"
)

def normalizar_uuid(valor: str | None) -> str | None:
    """
    - Remove whitespace (tabs, newlines, NBSP)
    - Remove qualquer caractere inválido
    - Retorna UUID canônico ou None
    """
    if valor is None:
        return None

    if not isinstance(valor, str):
        valor = str(valor)

    bruto = valor

    # remove qualquer whitespace invisível
    valor = re.sub(r"\s+", "", valor)

    # mantém apenas hex + hífen
    valor = re.sub(r"[^0-9a-fA-F-]", "", valor)

    if not valor:
        return None

    try:
        uid = str(UUID(valor))
    except Exception:
        return None

    if not _UUID_RE.match(uid):
        return None

    if bruto != uid:
        logger.warning(f"⚠️ input_id normalizado: bruto='{bruto}' -> uuid='{uid}'")

    return uid


# ============================================================
# 🚀 Função principal do worker
# ============================================================

def processar_pdv(tenant_id, file_path, descricao):
    job = get_current_job()
    writer = DatabaseWriter()

    # --------------------------------------------------------
    # Job ID seguro
    # --------------------------------------------------------
    job_id = str(job.id) if job and job.id else str(uuid4())

    logger.info(f"🚀 Iniciando job {job_id} | tenant={tenant_id}")

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

            # NÃO logar JSON
            if not line.lstrip().startswith("{"):
                logger.info(f"[MAIN] {line}")

            try:
                obj = json.loads(line)

                if isinstance(obj, dict) and obj.get("event") == "progress":
                    if job:
                        job.meta.update({
                            "progress": obj.get("pct", 0),
                            "step": obj.get("step", "")
                        })
                        job.save_meta()
                    continue

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
            logger.error("⚠️ JSON final não capturado, mas processo concluiu")
            resumo = {
                "status": "success",
                "arquivo": file_path,
                "total_processados": 0,
                "validos": 0,
                "invalidos": 0,
                "inseridos": 0,
                "mensagem": "JSON final não capturado"
            }

        # ----------------------------------------------------
        # Normaliza input_id
        # ----------------------------------------------------
        input_id_norm = normalizar_uuid(resumo.get("input_id"))

        logger.info(
            f"✅ Job {job_id}: "
            f"{resumo.get('validos', 0)} válidos / "
            f"{resumo.get('invalidos', 0)} inválidos | "
            f"input_id={input_id_norm or 'NULL'}"
        )

        # ----------------------------------------------------
        # Salva histórico (FONTE DA VERDADE)
        # ----------------------------------------------------
        writer.salvar_historico_pdv_job(
            tenant_id=tenant_id,
            job_id=job_id,
            arquivo=resumo.get("arquivo"),
            status=resumo.get("status"),
            total_processados=int(resumo.get("total_processados") or 0),
            validos=int(resumo.get("validos") or 0),
            invalidos=int(resumo.get("invalidos") or 0),
            inseridos=int(resumo.get("inseridos") or 0),
            sobrescritos=int(resumo.get("sobrescritos") or 0),
            arquivo_invalidos=resumo.get("arquivo_invalidos"),
            mensagem=resumo.get("mensagem"),
            descricao=descricao,
            input_id=input_id_norm,
        )

        if job:
            job.meta.update({"step": "Finalizado", "progress": 100})
            job.save_meta()

        return {
            "status": resumo.get("status"),
            "job_id": job_id,
            "tenant_id": tenant_id,
            "input_id": input_id_norm,
        }

    # ========================================================
    # ❌ ERRO
    # ========================================================
    except Exception as e:
        logger.error(f"💥 Erro no job {job_id}: {e}", exc_info=True)

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

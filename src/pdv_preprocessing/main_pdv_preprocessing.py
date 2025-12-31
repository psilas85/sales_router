# ============================================================
# 📦 src/pdv_preprocessing/main_pdv_preprocessing.py
#     ➜ VERSÃO FINAL COM PROGRESSO TEMPO REAL (MODELO A)
# ============================================================

import os
import argparse
import logging
import uuid
import time
import json
import sys
import re
from dotenv import load_dotenv

from pdv_preprocessing.application.pdv_preprocessing_use_case import PDVPreprocessingUseCase
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from pdv_preprocessing.utils.file_utils import detectar_separador, salvar_invalidos
from pdv_preprocessing.logs.logging_config import setup_logging


# ============================================================
# 🧼 Helpers
# ============================================================

_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)

def uuid_canonico(valor: str | None = None) -> str:
    """
    Gera/garante UUID em formato canônico.
    - Se valor None: gera uuid4()
    - Se valor existir: tenta parsear e devolver canônico
    """
    if valor is None:
        return str(uuid.uuid4())

    try:
        u = str(uuid.UUID(str(valor)))
        if not _UUID_RE.match(u):
            return str(uuid.uuid4())
        return u
    except Exception:
        return str(uuid.uuid4())


# ============================================================
# 🔵 Função auxiliar para emitir progresso
# ============================================================
def emit_progress(pct, step):
    """Evento de progresso consumido pelo worker."""
    obj = {"event": "progress", "pct": int(pct), "step": str(step)}
    print(json.dumps(obj, ensure_ascii=False))
    sys.stdout.flush()


# ============================================================
# 🔵 Função auxiliar para emitir JSON final
# ============================================================
def emit_final(obj):
    print(json.dumps(obj, ensure_ascii=False))
    sys.stdout.flush()


# ============================================================
# 🚀 Main
# ============================================================
def main():
    parser = argparse.ArgumentParser(
        description="Pré-processamento de PDVs (SalesRouter multi-tenant)"
    )
    parser.add_argument("--tenant", required=True)
    parser.add_argument("--arquivo", required=True)
    parser.add_argument("--descricao", required=True)
    parser.add_argument("--usar_google", action="store_true", default=True)

    args = parser.parse_args()

    try:
        tenant_id = int(args.tenant)
    except Exception:
        emit_final({"status": "error", "erro": "Tenant inválido"})
        return

    descricao = (args.descricao or "").strip()[:60]
    input_id = uuid_canonico()  # ✅ sempre canônico
    input_path = (args.arquivo or "").strip()

    load_dotenv()
    setup_logging(tenant_id)

    logging.info(f"🚀 Iniciando pré-processamento | tenant={tenant_id}")
    logging.info(f"🆔 input_id={input_id}")
    logging.info(f"📄 arquivo={input_path}")

    # ============================================================
    # 0% → Arquivo existe?
    # ============================================================
    emit_progress(1, "Verificando arquivo")
    if not input_path or not os.path.exists(input_path):
        emit_final({
            "status": "error",
            "erro": f"Arquivo não encontrado: {input_path}",
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao
        })
        return

    # ============================================================
    # Detectar separador (5%)
    # ============================================================
    emit_progress(5, "Detectando separador do CSV")
    try:
        sep = detectar_separador(input_path)
    except Exception as e:
        emit_final({
            "status": "error",
            "erro": f"Falha ao detectar separador: {e}",
            "tenant_id": tenant_id,
            "input_id": input_id
        })
        return

    inicio_execucao = time.time()

    # ============================================================
    # Abrir DB (10%)
    # ============================================================
    emit_progress(10, "Inicializando conexão com banco")
    try:
        db_reader = DatabaseReader()
        db_writer = DatabaseWriter()
    except Exception as e:
        emit_final({
            "status": "error",
            "erro": f"Falha DB: {e}",
            "tenant_id": tenant_id,
            "input_id": input_id
        })
        return

    # ============================================================
    # EXECUÇÃO PRINCIPAL (20% → 90%)
    # ============================================================
    try:
        emit_progress(20, "Executando pré-processamento")

        use_case = PDVPreprocessingUseCase(
            reader=db_reader,
            writer=db_writer,
            tenant_id=tenant_id,
            input_id=input_id,
            descricao=descricao,
            usar_google=args.usar_google
        )

        df_validos, df_invalidos, inseridos = use_case.execute(input_path, sep)

        total_validos = len(df_validos)
        total_invalidos = len(df_invalidos)
        total = total_validos + total_invalidos

        # ============================================================
        # Salvando inválidos (90%)
        # ============================================================
        emit_progress(90, "Salvando registros inválidos")
        arquivo_invalidos = salvar_invalidos(
            df_invalidos,
            os.path.dirname(input_path),
            input_id
        )

        # ============================================================
        # Finalização (99%)
        # ============================================================
        emit_progress(99, "Finalizando execução")

        duracao = time.time() - inicio_execucao

        resultado = {
            "status": "done",
            "tenant_id": tenant_id,
            "input_id": input_id,  # ✅ canônico
            "descricao": descricao,
            "arquivo": os.path.basename(input_path),
            "total_processados": total,
            "validos": total_validos,
            "invalidos": total_invalidos,
            "inseridos": inseridos,
            "arquivo_invalidos": arquivo_invalidos,
            "duracao_segundos": round(duracao, 2),
        }

        emit_final(resultado)

    except Exception as e:
        logging.error(f"💥 Erro inesperado: {e}", exc_info=True)
        emit_final({
            "status": "error",
            "erro": str(e),
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao
        })


if __name__ == "__main__":
    main()


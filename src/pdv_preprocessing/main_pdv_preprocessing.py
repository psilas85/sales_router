#sales_router/src/pdv_preprocessing/main_pdv_preprocessing.py

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
    if valor is None:
        return str(uuid.uuid4())
    try:
        u = str(uuid.UUID(str(valor)))
        return u if _UUID_RE.match(u) else str(uuid.uuid4())
    except Exception:
        return str(uuid.uuid4())


# ============================================================
# 🔵 Emissão de progresso
# ============================================================

def emit_progress(pct, step):
    print(json.dumps({
        "event": "progress",
        "pct": int(pct),
        "step": str(step)
    }, ensure_ascii=False))
    sys.stdout.flush()


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

    # --------------------------------------------------------
    # Tenant
    # --------------------------------------------------------
    try:
        tenant_id = int(args.tenant)
    except Exception:
        emit_final({"status": "error", "erro": "Tenant inválido"})
        return

    descricao = (args.descricao or "").strip()[:60]
    input_id = uuid_canonico()
    input_path = (args.arquivo or "").strip()

    load_dotenv()
    setup_logging(tenant_id)

    logging.info(f"🚀 Iniciando pré-processamento | tenant={tenant_id}")
    logging.info(f"🆔 input_id={input_id}")
    logging.info(f"📄 arquivo={input_path}")

    # --------------------------------------------------------
    # Arquivo existe?
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # Tipo de arquivo
    # --------------------------------------------------------
    ext = os.path.splitext(input_path)[1].lower()
    sep = None

    if ext not in [".xlsx", ".xls"]:
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
    else:
        emit_progress(5, "Arquivo XLSX detectado")

    inicio_execucao = time.time()

    # --------------------------------------------------------
    # Abrir banco
    # --------------------------------------------------------
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

    # --------------------------------------------------------
    # Execução principal
    # --------------------------------------------------------
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

        df_validos, df_invalidos, inseridos = use_case.execute(
            input_path=input_path,
            sep=sep
        )

        total_validos = len(df_validos)
        total_invalidos = len(df_invalidos)
        total = total_validos + total_invalidos

        # --------------------------------------------------------
        # Salvar inválidos
        # --------------------------------------------------------
        emit_progress(90, "Salvando registros inválidos")
        arquivo_invalidos = salvar_invalidos(
            df_invalidos,
            os.path.dirname(input_path),
            input_id
        )

        # --------------------------------------------------------
        # Finalização
        # --------------------------------------------------------
        emit_progress(99, "Finalizando execução")

        duracao = round(time.time() - inicio_execucao, 2)

        emit_final({
            "status": "done",
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao,
            "arquivo": os.path.basename(input_path),
            "total_processados": total,
            "validos": total_validos,
            "invalidos": total_invalidos,
            "inseridos": inseridos,
            "arquivo_invalidos": arquivo_invalidos,
            "duracao_segundos": duracao
        })

    except Exception as e:
        logging.error("💥 Erro inesperado", exc_info=True)
        emit_final({
            "status": "error",
            "erro": str(e),
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao
        })


if __name__ == "__main__":
    main()

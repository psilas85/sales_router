# ============================================================
# 📦 src/pdv_preprocessing/main_mkp_preprocessing.py
# ============================================================

import os
import argparse
import logging
import uuid
import time
import json
from dotenv import load_dotenv

from pdv_preprocessing.application.mkp_preprocessing_use_case import MKPPreprocessingUseCase
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from database.db_connection import get_connection
from pdv_preprocessing.logs.logging_config import setup_logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()


# ------------------------------------------------------------
# Detecta separador CSV
# ------------------------------------------------------------
def detectar_separador(path: str) -> str:
    with open(path, "r", encoding="utf-8-sig") as f:
        linha = f.readline()
        return ";" if ";" in linha else ","


# ------------------------------------------------------------
# Salva inválidos em CSV
# ------------------------------------------------------------
def salvar_invalidos(df_invalidos, pasta_base: str, input_id: str):
    try:
        if df_invalidos is None or df_invalidos.empty:
            return None
        pasta_invalidos = os.path.join(pasta_base, "invalidos")
        os.makedirs(pasta_invalidos, exist_ok=True)
        nome_arquivo = f"mkp_invalidos_{input_id}.csv"
        caminho_saida = os.path.join(pasta_invalidos, nome_arquivo)
        df_invalidos.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8-sig")
        logging.warning(f"⚠️ {len(df_invalidos)} inválidos salvos em: {caminho_saida}")
        return caminho_saida
    except Exception as e:
        logging.error(f"❌ Erro ao salvar inválidos: {e}")
        return None


# ------------------------------------------------------------
# Execução principal
# ------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Pré-processamento de dados agregados por CEP (Marketplace)."
    )
    parser.add_argument("--tenant", required=True, help="Tenant ID (inteiro)")
    parser.add_argument("--arquivo", required=True, help="Caminho do CSV de entrada")
    parser.add_argument("--descricao", required=True, help="Descrição do processamento")
    args = parser.parse_args()

    try:
        tenant_id = int(args.tenant)
    except ValueError:
        logging.error("❌ Tenant ID inválido. Deve ser um número inteiro.")
        return

    descricao = args.descricao.strip()[:60]
    input_id = str(uuid.uuid4())

    setup_logging(tenant_id)
    logging.info(f"🚀 Iniciando pré-processamento MKP (tenant={tenant_id})")
    logging.info(f"🆔 input_id={input_id}")
    logging.info(f"📝 Descrição: {descricao}")
    logging.info("⚙️ Modo de execução: paralelizado (Nominatim + cache global + fallback Google)")

    input_path = args.arquivo
    if not os.path.exists(input_path):
        logging.error(f"❌ Arquivo não encontrado: {input_path}")
        return

    sep = detectar_separador(input_path)
    inicio_execucao = time.time()

    try:
        conn = get_connection()
        db_reader = DatabaseReader(conn)
        db_writer = DatabaseWriter(conn)
    except Exception as e:
        logging.error(f"❌ Falha ao conectar ao banco: {e}", exc_info=True)
        return

    try:
        # ============================================================
        # 🚀 Executa pipeline principal
        # ============================================================
        use_case = MKPPreprocessingUseCase(
            db_reader,
            db_writer,
            tenant_id,
            input_id=input_id,
            descricao=descricao
        )

        df_validos, df_invalidos, inseridos = use_case.execute(
            input_path=input_path,
            sep=sep,
            input_id=input_id,
            descricao=descricao,
        )

        total_validos = len(df_validos) if df_validos is not None else 0
        total_invalidos = len(df_invalidos) if df_invalidos is not None else 0
        total = total_validos + total_invalidos

        arquivo_invalidos = salvar_invalidos(df_invalidos, os.path.dirname(input_path), input_id)
        duracao = time.time() - inicio_execucao

        # ============================================================
        # 💾 Registro de histórico no banco
        # ============================================================
        db_writer.salvar_historico_pdv_job(
            tenant_id=tenant_id,
            input_id=input_id,
            descricao=descricao,
            arquivo=os.path.basename(input_path),
            status="done",
            total_processados=total,
            validos=total_validos,
            invalidos=total_invalidos,
            arquivo_invalidos=arquivo_invalidos,
            mensagem="✅ Pré-processamento MKP concluído com sucesso",
            inseridos=inseridos,
        )

        # ============================================================
        # 📊 Resumo final da execução
        # ============================================================
        logging.info("📊 Resumo da execução:")
        logging.info(f"   • Tenant: {tenant_id}")
        logging.info(f"   • Input ID: {input_id}")
        logging.info(f"   • Descrição: {descricao}")
        logging.info(f"   • CEPs válidos: {total_validos}")
        logging.info(f"   • CEPs inválidos: {total_invalidos}")
        logging.info(f"   • Registros inseridos: {inseridos}")
        logging.info(f"   • Threads ativas: {use_case.geo_service.max_workers}")
        logging.info(f"   • Tempo total: {duracao:.2f}s")
        logging.info("✅ Processo MKP finalizado com sucesso.")

        # ============================================================
        # 🧾 Saída JSON para integração com orquestrador
        # ============================================================
        print(json.dumps({
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
            "duracao_segundos": round(duracao, 2)
        }))

    except Exception as e:
        logging.error(f"❌ Erro inesperado durante o processamento MKP: {e}", exc_info=True)
        try:
            db_writer.salvar_historico_pdv_job(
                tenant_id=tenant_id,
                input_id=input_id,
                descricao=descricao,
                arquivo=os.path.basename(input_path),
                status="error",
                total_processados=0,
                validos=0,
                invalidos=0,
                arquivo_invalidos=None,
                mensagem=str(e),
                inseridos=0,
            )
        except Exception as err:
            logging.error(f"⚠️ Falha ao registrar histórico de erro: {err}")

        print(json.dumps({
            "status": "error",
            "erro": str(e),
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao
        }))

    finally:
        try:
            conn.commit()
            conn.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()

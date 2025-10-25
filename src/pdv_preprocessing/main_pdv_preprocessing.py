import os
import argparse
import logging
import uuid
import time
import json
from dotenv import load_dotenv

from pdv_preprocessing.application.pdv_preprocessing_use_case import PDVPreprocessingUseCase
from pdv_preprocessing.infrastructure.database_reader import DatabaseReader
from pdv_preprocessing.infrastructure.database_writer import DatabaseWriter
from database.db_connection import get_connection
from pdv_preprocessing.logs.logging_config import setup_logging

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
load_dotenv()


def detectar_separador(path: str) -> str:
    """Detecta automaticamente o separador do CSV."""
    with open(path, "r", encoding="utf-8-sig") as f:
        linha = f.readline()
        return ";" if ";" in linha else ","


def salvar_invalidos(df_invalidos, pasta_base: str, input_id: str):
    """Salva PDVs inválidos em CSV e retorna o caminho."""
    try:
        if df_invalidos is None or df_invalidos.empty:
            return None
        pasta_invalidos = os.path.join(pasta_base, "invalidos")
        os.makedirs(pasta_invalidos, exist_ok=True)
        nome_arquivo = f"pdvs_invalidos_{input_id}.csv"
        caminho_saida = os.path.join(pasta_invalidos, nome_arquivo)
        df_invalidos.to_csv(caminho_saida, index=False, sep=";", encoding="utf-8-sig")
        logging.warning(f"⚠️ {len(df_invalidos)} inválidos salvos em: {caminho_saida}")
        return caminho_saida
    except Exception as e:
        logging.error(f"❌ Erro ao salvar inválidos: {e}")
        return None


def main():
    parser = argparse.ArgumentParser(description="Pré-processamento de PDVs (SalesRouter multi-tenant)")
    parser.add_argument("--tenant", required=True, help="Tenant ID (inteiro ou variável TENANT_ID do .env)")
    parser.add_argument("--arquivo", required=True, help="Caminho do CSV de entrada (ex: /app/data/pdvs_enderecos.csv)")
    parser.add_argument("--descricao", required=True, help="Descrição do processamento (máx. 60 caracteres)")
    args = parser.parse_args()

    # ------------------------------------------------------------
    # 🔹 Inicialização de variáveis
    # ------------------------------------------------------------
    try:
        tenant_id = int(args.tenant or os.getenv("TENANT_ID"))
    except (TypeError, ValueError):
        logging.error("❌ Tenant ID inválido ou ausente.")
        return

    descricao = args.descricao.strip()[:60]
    input_id = str(uuid.uuid4())

    # ------------------------------------------------------------
    # 🧾 Logging e informações iniciais
    # ------------------------------------------------------------
    setup_logging(tenant_id)
    logging.info(f"🚀 Iniciando pré-processamento de PDVs (tenant={tenant_id})")
    logging.info(f"🆔 input_id={input_id}")
    logging.info(f"📝 Descrição: {descricao}")

    input_path = args.arquivo
    if not os.path.exists(input_path):
        logging.error(f"❌ Arquivo não encontrado: {input_path}")
        return

    sep = detectar_separador(input_path)
    inicio_execucao = time.time()

    # ------------------------------------------------------------
    # 🔗 Conexão com banco
    # ------------------------------------------------------------
    try:
        conn = get_connection()
        db_reader = DatabaseReader(conn)
        db_writer = DatabaseWriter(conn)
    except Exception as e:
        logging.error(f"❌ Falha ao conectar ao banco: {e}")
        return

    # ------------------------------------------------------------
    # 🚀 Execução principal
    # ------------------------------------------------------------
    try:
        use_case = PDVPreprocessingUseCase(
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

        logging.info(f"✅ {total_validos} válidos / {total_invalidos} inválidos processados.")
        logging.info(f"💾 {inseridos} PDVs gravados no banco.")
        logging.info(f"⏱️ Duração total: {duracao:.2f}s")

        # --------------------------------------------------------
        # 🧾 Registro do histórico
        # --------------------------------------------------------
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
            mensagem="✅ Pré-processamento de PDVs concluído com sucesso",
            inseridos=inseridos,
        )

        # --------------------------------------------------------
        # 📤 Saída JSON
        # --------------------------------------------------------
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
        logging.error(f"❌ Erro inesperado: {e}", exc_info=True)
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

        print(json.dumps({
            "status": "error",
            "erro": str(e),
            "tenant_id": tenant_id,
            "input_id": input_id,
            "descricao": descricao
        }))


if __name__ == "__main__":
    main()

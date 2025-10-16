# src/database/cleanup_service.py

from src.database.db_connection import get_connection
from loguru import logger


def limpar_dados_operacionais(nivel: str, tenant_id: int | None = None):
    """
    Limpa os dados operacionais do SalesRouter conforme o nível de execução.

    Parâmetros:
        nivel: 'preprocessing' | 'clusterization' | 'routing'
        tenant_id: se informado, filtra apenas os dados do tenant específico
    """
    conn = get_connection()
    cur = conn.cursor()

    # Define quais tabelas serão afetadas por nível
    if nivel == "preprocessing":
        tabelas = [
            "sales_subcluster_pdv",
            "sales_subcluster",
            "cluster_setor_pdv",
            "cluster_setor",
            "cluster_run",
        ]
        logger.info("🧹 Limpando dados operacionais (nível: pré-processamento).")

    elif nivel == "clusterization":
        tabelas = [
            "sales_subcluster_pdv",
            "sales_subcluster",
            "cluster_setor_pdv",
            "cluster_setor",
            "cluster_run",
        ]
        logger.info("🧹 Limpando dados operacionais (nível: clusterização).")

    elif nivel == "routing":
        tabelas = ["sales_subcluster_pdv", "sales_subcluster"]
        logger.info("🧹 Limpando dados operacionais (nível: roteirização).")

    else:
        raise ValueError(f"Nível de limpeza inválido: {nivel}")

    # Executa limpeza com segurança — filtrando por tenant se informado
    tabelas_limpeza = []
    try:
        for tabela in tabelas:
            if tenant_id:
                cur.execute(f"DELETE FROM {tabela} WHERE tenant_id = %s;", (tenant_id,))
                logger.debug(f"🧹 Linhas removidas da tabela '{tabela}' (tenant_id={tenant_id})")
            else:
                cur.execute(f"TRUNCATE TABLE {tabela} CASCADE;")
                logger.debug(f"🧹 Tabela '{tabela}' truncada completamente (sem filtro de tenant).")

            tabelas_limpeza.append(tabela)

        conn.commit()
        logger.success(
            f"✅ Limpeza concluída para {len(tabelas_limpeza)} tabela(s): "
            f"{', '.join(tabelas_limpeza)}. Snapshots, históricos e caches preservados."
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"❌ Erro ao limpar dados operacionais ({nivel}): {e}", exc_info=True)

    finally:
        cur.close()
        conn.close()

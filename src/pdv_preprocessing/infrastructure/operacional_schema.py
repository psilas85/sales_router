# sales_router/src/pdv_preprocessing/infrastructure/operacional_schema.py
#
# Lazy migration do schema `operacional` — espelho das tabelas de carregamento
# da Simulação Inteligente (public.pdvs / historico_pdv_jobs / pdv_invalidos),
# usado pela pipeline de Execução Operacional.
#
# As tabelas são criadas com `LIKE public.<tabela> INCLUDING ALL`, garantindo
# clone exato de colunas, defaults, índices e constraints no momento da criação.
# O cache de geocodificação (enderecos_cache) e o cadastro de clientes
# (cadastro_pdvs) NÃO são clonados — continuam só em `public`, compartilhados;
# com search_path = operacional, public eles resolvem nativamente.

import logging

_OPERACIONAL_SCHEMA_ENSURED = False

_DDL = """
CREATE SCHEMA IF NOT EXISTS operacional;

CREATE TABLE IF NOT EXISTS operacional.pdvs
    (LIKE public.pdvs INCLUDING ALL);
CREATE TABLE IF NOT EXISTS operacional.historico_pdv_jobs
    (LIKE public.historico_pdv_jobs INCLUDING ALL);
CREATE TABLE IF NOT EXISTS operacional.pdv_invalidos
    (LIKE public.pdv_invalidos INCLUDING ALL);
"""

# O LIKE copia o DEFAULT nextval() apontando para a sequência de `public`.
# Aqui damos a cada tabela operacional sua própria sequência.
_SEQUENCES = [
    ("operacional.pdvs_id_seq", "operacional.pdvs"),
    ("operacional.historico_pdv_jobs_id_seq", "operacional.historico_pdv_jobs"),
    ("operacional.pdv_invalidos_id_seq", "operacional.pdv_invalidos"),
]

# Trigger de atualizado_em — não é copiado pelo LIKE. Reusa a função de public.
_TRIGGER = """
DROP TRIGGER IF EXISTS trg_update_pdvs_timestamp ON operacional.pdvs;
CREATE TRIGGER trg_update_pdvs_timestamp
    BEFORE UPDATE ON operacional.pdvs
    FOR EACH ROW EXECUTE FUNCTION public.update_pdvs_timestamp();
"""


def ensure_operacional_schema(conn) -> None:
    """Cria (idempotente) o schema operacional e suas tabelas."""
    global _OPERACIONAL_SCHEMA_ENSURED
    if _OPERACIONAL_SCHEMA_ENSURED:
        return
    with conn.cursor() as cur:
        cur.execute(_DDL)
        for seq, tabela in _SEQUENCES:
            cur.execute(
                f"CREATE SEQUENCE IF NOT EXISTS {seq} OWNED BY {tabela}.id;"
            )
            cur.execute(
                f"ALTER TABLE {tabela} ALTER COLUMN id SET DEFAULT nextval('{seq}');"
            )
            # Alinha a sequência ao maior id já existente (idempotência).
            cur.execute(
                f"SELECT setval('{seq}', "
                f"COALESCE((SELECT MAX(id) FROM {tabela}), 1), "
                f"(SELECT COUNT(*) > 0 FROM {tabela}));"
            )
        cur.execute(_TRIGGER)
    conn.commit()
    _OPERACIONAL_SCHEMA_ENSURED = True
    logging.info("[OPERACIONAL] schema operacional garantido.")

# sales_router/src/sales_clusterization/infrastructure/operacional_cluster_schema.py
#
# Lazy migration do schema `operacional` para a SETORIZAÇÃO — espelho das
# tabelas cluster_run / cluster_setor / cluster_setor_pdv de `public`.
#
# Diferenças do clone:
#  - operacional.cluster_setor ganha consultor_id / consultor_nome — cada
#    setor operacional é um consultor cadastrado (o centro do setor).
#  - cluster_run ganha `desatualizado` (modelo "stale": setorização fica
#    desatualizada quando o carregamento muda) — aplicado nos DOIS ambientes.
#
# As tabelas-globais (consultores, cadastro_pdvs, enderecos_cache) NÃO são
# clonadas — seguem em `public`, resolvidas via search_path.

import logging

_OPERACIONAL_CLUSTER_SCHEMA_ENSURED = False

_DDL = """
CREATE SCHEMA IF NOT EXISTS operacional;

CREATE TABLE IF NOT EXISTS operacional.cluster_run
    (LIKE public.cluster_run INCLUDING ALL);
CREATE TABLE IF NOT EXISTS operacional.cluster_setor
    (LIKE public.cluster_setor INCLUDING ALL);
CREATE TABLE IF NOT EXISTS operacional.cluster_setor_pdv
    (LIKE public.cluster_setor_pdv INCLUDING ALL);
"""

# cluster_run e cluster_setor têm id bigserial — sequência própria por schema.
# cluster_setor_pdv tem PK composta (run_id, pdv_id), sem sequência.
_SEQUENCES = [
    ("operacional.cluster_run_id_seq", "operacional.cluster_run"),
    ("operacional.cluster_setor_id_seq", "operacional.cluster_setor"),
]

_COLUNAS_EXTRA = """
ALTER TABLE operacional.cluster_setor
    ADD COLUMN IF NOT EXISTS consultor_id uuid;
ALTER TABLE operacional.cluster_setor
    ADD COLUMN IF NOT EXISTS consultor_nome text;
ALTER TABLE operacional.cluster_run
    ADD COLUMN IF NOT EXISTS desatualizado boolean NOT NULL DEFAULT false;
ALTER TABLE public.cluster_run
    ADD COLUMN IF NOT EXISTS desatualizado boolean NOT NULL DEFAULT false;
"""


def ensure_operacional_cluster_schema(conn) -> None:
    """Cria (idempotente) as tabelas de setorização do schema operacional."""
    global _OPERACIONAL_CLUSTER_SCHEMA_ENSURED
    if _OPERACIONAL_CLUSTER_SCHEMA_ENSURED:
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
            cur.execute(
                f"SELECT setval('{seq}', "
                f"COALESCE((SELECT MAX(id) FROM {tabela}), 1), "
                f"(SELECT COUNT(*) > 0 FROM {tabela}));"
            )
        cur.execute(_COLUNAS_EXTRA)
    conn.commit()
    _OPERACIONAL_CLUSTER_SCHEMA_ENSURED = True
    logging.info("[OPERACIONAL] schema operacional de setorização garantido.")

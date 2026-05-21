# sales_router/src/sales_routing/infrastructure/operacional_routing_schema.py
#
# Lazy migration do schema `operacional` para a ROTEIRIZAÇÃO — espelho das
# tabelas sales_subcluster / sales_subcluster_pdv / sales_routing_resumo /
# historico_subcluster_jobs de `public`.
#
# As tabelas-globais NÃO são clonadas: `pdvs` resolve a operacional.pdvs via
# search_path; `consultores`, `route_cache` e `agenda*` seguem em `public`.
# Os setores de origem (cluster_run/cluster_setor/cluster_setor_pdv) já
# existem em operacional — criados pela setorização operacional.
#
# Espelha sales_clusterization/infrastructure/operacional_cluster_schema.py.

import logging

_OPERACIONAL_ROUTING_SCHEMA_ENSURED = False

_DDL = """
CREATE SCHEMA IF NOT EXISTS operacional;

CREATE TABLE IF NOT EXISTS operacional.sales_subcluster
    (LIKE public.sales_subcluster INCLUDING ALL);
CREATE TABLE IF NOT EXISTS operacional.sales_subcluster_pdv
    (LIKE public.sales_subcluster_pdv INCLUDING ALL);
CREATE TABLE IF NOT EXISTS operacional.sales_routing_resumo
    (LIKE public.sales_routing_resumo INCLUDING ALL);
CREATE TABLE IF NOT EXISTS operacional.historico_subcluster_jobs
    (LIKE public.historico_subcluster_jobs INCLUDING ALL);
"""

# As 4 tabelas têm `id` serial. LIKE INCLUDING ALL copia o DEFAULT
# nextval('public.<seq>') — re-aponta cada uma para uma sequência própria
# do schema operacional, senão os ids colidiriam com os de public.
_TABELAS_COM_SEQ = [
    "operacional.sales_subcluster",
    "operacional.sales_subcluster_pdv",
    "operacional.sales_routing_resumo",
    "operacional.historico_subcluster_jobs",
]

# Views consumidas pelos relatórios XLSX (resumo/pdvs). LIKE não clona
# views — recriadas aqui com search_path=operacional para que os nomes
# não-qualificados do corpo vinculem às tabelas do schema operacional.
_VIEWS = """
CREATE OR REPLACE VIEW vw_sales_routing_resumo_cluster AS
WITH rotas AS (
    SELECT s.tenant_id, s.routing_id, s.cluster_id,
        round(avg(s.tempo_total_min)::numeric, 2) AS tempo_medio_min,
        round(max(s.tempo_total_min)::numeric, 2) AS tempo_max_min,
        round(sum(s.tempo_total_min)::numeric, 2) AS tempo_total_min,
        round(avg(s.dist_total_km)::numeric, 2) AS dist_media_km,
        round(max(s.dist_total_km)::numeric, 2) AS dist_max_km,
        round(sum(s.dist_total_km)::numeric, 2) AS dist_total_km,
        round(avg(s.centro_lat)::numeric, 6)::double precision AS centro_lat,
        round(avg(s.centro_lon)::numeric, 6)::double precision AS centro_lon
    FROM sales_subcluster s
    GROUP BY s.tenant_id, s.routing_id, s.cluster_id
), pdvs_cluster AS (
    SELECT sp.tenant_id, sp.routing_id, sp.cluster_id,
        count(DISTINCT sp.pdv_id) AS qtd_pdvs,
        round(sum(COALESCE(p_1.pdv_vendas, 0::double precision))::numeric, 2)
            AS valor_total_vendas
    FROM sales_subcluster_pdv sp
    JOIN pdvs p_1 ON p_1.id = sp.pdv_id AND p_1.tenant_id = sp.tenant_id
    GROUP BY sp.tenant_id, sp.routing_id, sp.cluster_id
)
SELECT r.tenant_id, r.routing_id, r.cluster_id, r.centro_lat, r.centro_lon,
    COALESCE(p.qtd_pdvs, 0::bigint) AS qtd_pdvs,
    COALESCE(p.valor_total_vendas, 0::numeric) AS valor_total_vendas,
    r.tempo_medio_min, r.tempo_max_min, r.tempo_total_min,
    r.dist_media_km, r.dist_max_km, r.dist_total_km
FROM rotas r
LEFT JOIN pdvs_cluster p
    ON p.tenant_id = r.tenant_id AND p.routing_id = r.routing_id
   AND p.cluster_id = r.cluster_id;

CREATE OR REPLACE VIEW vw_pdv_cluster_subcluster AS
SELECT p.id AS pdv_id, p.tenant_id, p.input_id, p.cnpj, p.logradouro,
    p.numero, p.bairro, p.cidade, p.uf, p.cep, p.pdv_endereco_completo,
    p.pdv_lat, p.pdv_lon, p.status_geolocalizacao, p.pdv_vendas,
    p.descricao AS input_descricao, p.criado_em, p.atualizado_em,
    cr.id AS cluster_run_id, cr.clusterization_id,
    cs.id AS cluster_id, cs.nome AS cluster_nome,
    cs.centro_lat AS cluster_centro_lat, cs.centro_lon AS cluster_centro_lon,
    sc.run_id AS subcluster_run_id, sc.routing_id,
    sc.subcluster_seq AS subcluster_numero, sp.sequencia_ordem AS ordem_rota,
    sc.criado_em::timestamp without time zone AS subcluster_criado_em
FROM sales_subcluster_pdv sp
JOIN sales_subcluster sc
    ON sc.tenant_id = sp.tenant_id AND sc.routing_id = sp.routing_id
   AND sc.cluster_id = sp.cluster_id
   AND sc.subcluster_seq = sp.subcluster_seq
JOIN cluster_setor cs ON cs.id = sp.cluster_id
JOIN cluster_run cr ON cr.id = sc.run_id
JOIN pdvs p ON p.id = sp.pdv_id;
"""


def ensure_operacional_routing_schema(conn) -> None:
    """Cria (idempotente) as tabelas de roteirização do schema operacional."""
    global _OPERACIONAL_ROUTING_SCHEMA_ENSURED
    if _OPERACIONAL_ROUTING_SCHEMA_ENSURED:
        return
    with conn.cursor() as cur:
        cur.execute(_DDL)
        for tabela in _TABELAS_COM_SEQ:
            seq = f"{tabela}_id_seq"
            cur.execute(
                f"CREATE SEQUENCE IF NOT EXISTS {seq} OWNED BY {tabela}.id;"
            )
            cur.execute(
                f"ALTER TABLE {tabela} "
                f"ALTER COLUMN id SET DEFAULT nextval('{seq}');"
            )
            cur.execute(
                f"SELECT setval('{seq}', "
                f"COALESCE((SELECT MAX(id) FROM {tabela}), 1), "
                f"(SELECT COUNT(*) > 0 FROM {tabela}));"
            )
        # search_path operacional → as views (nomes não-qualificados) são
        # criadas em operacional e o corpo vincula às tabelas operacionais.
        cur.execute("SET search_path TO operacional, public")
        cur.execute(_VIEWS)
    conn.commit()
    _OPERACIONAL_ROUTING_SCHEMA_ENSURED = True
    logging.info("[OPERACIONAL] schema operacional de roteirização garantido.")

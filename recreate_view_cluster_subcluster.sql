CREATE OR REPLACE VIEW vw_pdv_cluster_subcluster AS
SELECT
    p.id AS pdv_id,
    p.tenant_id,
    p.input_id,
    p.cnpj,
    p.logradouro,
    p.numero,
    p.bairro,
    p.cidade,
    p.uf,
    p.cep,
    p.pdv_endereco_completo,
    p.pdv_lat,
    p.pdv_lon,
    p.status_geolocalizacao,
    p.pdv_vendas,
    p.descricao AS input_descricao,
    p.criado_em,
    p.atualizado_em,

    -- Cluster principal
    csp.run_id AS cluster_run_id,
    cr.clusterization_id,
    csp.cluster_id AS cluster_id,
    cs.nome AS cluster_nome,
    cs.centro_lat AS cluster_centro_lat,
    cs.centro_lon AS cluster_centro_lon,

    -- Subcluster / rota
    ssp.run_id AS subcluster_run_id,
    ssp.routing_id,
    ssp.subcluster_seq AS subcluster_numero,
    ssp.sequencia_ordem AS ordem_rota,
    ssp.criado_em AS subcluster_criado_em

FROM pdvs p
LEFT JOIN cluster_setor_pdv csp 
    ON p.id = csp.pdv_id 
    AND p.tenant_id = csp.tenant_id
LEFT JOIN cluster_setor cs 
    ON cs.id = csp.cluster_id 
    AND cs.tenant_id = csp.tenant_id
LEFT JOIN cluster_run cr 
    ON cr.id = csp.run_id 
    AND cr.tenant_id = p.tenant_id
LEFT JOIN sales_subcluster_pdv ssp 
    ON p.id = ssp.pdv_id 
    AND p.tenant_id = ssp.tenant_id
    AND ssp.run_id = csp.run_id;

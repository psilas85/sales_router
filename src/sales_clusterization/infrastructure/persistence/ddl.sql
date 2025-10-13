-- sales_clusterization/infrastructure/persistence/ddl.sql

-- Execuções de clusterização (auditoria)
CREATE TABLE IF NOT EXISTS cluster_run (
  id BIGSERIAL PRIMARY KEY,
  started_at TIMESTAMP NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMP,
  uf TEXT,
  cidade TEXT,
  k_final INT,
  algo TEXT,
  params JSONB,
  status TEXT,
  error TEXT
);

-- Setores (macroclusters)
CREATE TABLE IF NOT EXISTS cluster_setor (
  id BIGSERIAL PRIMARY KEY,
  run_id BIGINT NOT NULL REFERENCES cluster_run(id),
  cluster_label INT NOT NULL,
  nome TEXT,
  centro_lat DOUBLE PRECISION,
  centro_lon DOUBLE PRECISION,
  n_pdvs INT NOT NULL,
  metrics JSONB
);

-- Mapeamento PDV -> Setor
CREATE TABLE IF NOT EXISTS cluster_setor_pdv (
  run_id BIGINT NOT NULL REFERENCES cluster_run(id),
  cluster_id BIGINT NOT NULL REFERENCES cluster_setor(id),
  pdv_id BIGINT NOT NULL,
  lat DOUBLE PRECISION,
  lon DOUBLE PRECISION,
  cidade TEXT,
  uf TEXT,
  PRIMARY KEY (run_id, pdv_id)
);

CREATE INDEX IF NOT EXISTS idx_cluster_setor_run ON cluster_setor(run_id);
CREATE INDEX IF NOT EXISTS idx_cluster_setor_pdv_cluster ON cluster_setor_pdv(cluster_id);

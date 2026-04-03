-- Optimizer Service Schema
-- Compatible with MySQL (production) and H2 in MySQL mode (tests).
CREATE TABLE IF NOT EXISTS table_operations (
  id             VARCHAR(36)   NOT NULL,
  table_uuid     VARCHAR(36)   NOT NULL,
  database_name  VARCHAR(255)  NOT NULL,
  table_name     VARCHAR(255)  NOT NULL,
  operation_type VARCHAR(50)   NOT NULL,
  status         VARCHAR(20)   NOT NULL,
  created_at     TIMESTAMP(6)  NOT NULL,
  scheduled_at   TIMESTAMP(6),
  job_id         VARCHAR(255),
  version        BIGINT,
  metrics        TEXT,
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS table_stats (
  table_uuid       VARCHAR(36)   NOT NULL,
  database_id      VARCHAR(255)  NOT NULL,
  table_name       VARCHAR(255)  NOT NULL,
  stats            TEXT,
  table_properties TEXT,
  updated_at       TIMESTAMP(6)  NOT NULL,
  PRIMARY KEY (table_uuid)
);

CREATE TABLE IF NOT EXISTS table_stats_history (
  id             BIGINT        NOT NULL AUTO_INCREMENT,
  table_uuid     VARCHAR(36)   NOT NULL,
  database_id    VARCHAR(255)  NOT NULL,
  table_name     VARCHAR(255)  NOT NULL,
  stats          TEXT,
  recorded_at    TIMESTAMP(6)  NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_tsh_table_uuid (table_uuid),
  INDEX idx_tsh_recorded_at (recorded_at)
);

CREATE TABLE IF NOT EXISTS table_operations_history (
  id             VARCHAR(36)   NOT NULL,
  table_uuid     VARCHAR(36)   NOT NULL,
  database_name  VARCHAR(255)  NOT NULL,
  table_name     VARCHAR(255)  NOT NULL,
  operation_type VARCHAR(50)   NOT NULL,
  submitted_at   TIMESTAMP(6)  NOT NULL,
  status         VARCHAR(20)   NOT NULL,
  job_id                VARCHAR(255),
  result                TEXT,
  orphan_files_deleted  INT,
  orphan_bytes_deleted  BIGINT,
  PRIMARY KEY (id)
);

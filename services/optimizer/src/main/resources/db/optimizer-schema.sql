-- Optimizer Service Schema
-- Compatible with MySQL (production) and H2 in MySQL mode (tests).
CREATE TABLE IF NOT EXISTS table_operations (
  id             VARCHAR(36)   NOT NULL,
  table_uuid     VARCHAR(36)   NOT NULL,
  database_name  VARCHAR(128)  NOT NULL,
  table_name     VARCHAR(128)  NOT NULL,
  operation_type VARCHAR(50)   NOT NULL,
  status         VARCHAR(20)   NOT NULL,
  created_at     TIMESTAMP(6)  NOT NULL,
  scheduled_at   TIMESTAMP(6),
  job_id         VARCHAR(255),
  -- TODO: per-operation metric columns will be added as operations are onboarded.
  PRIMARY KEY (id)
);

CREATE TABLE IF NOT EXISTS table_stats (
  table_uuid       VARCHAR(36)   NOT NULL,
  database_name    VARCHAR(128)  NOT NULL,
  table_name       VARCHAR(128)  NOT NULL,
  snapshot         TEXT,
  table_properties TEXT,
  updated_at       TIMESTAMP(6)  NOT NULL,
  PRIMARY KEY (table_uuid)
);

CREATE TABLE IF NOT EXISTS table_stats_history (
  id             VARCHAR(36)   NOT NULL,
  table_uuid     VARCHAR(36)   NOT NULL,
  database_name  VARCHAR(128)  NOT NULL,
  table_name     VARCHAR(128)  NOT NULL,
  snapshot       TEXT,
  delta          TEXT,
  recorded_at    TIMESTAMP(6)  NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_tsh_table_uuid (table_uuid),
  INDEX idx_tsh_recorded_at (recorded_at)
);

CREATE TABLE IF NOT EXISTS table_operations_history (
  id                    VARCHAR(36)   NOT NULL,
  table_uuid            VARCHAR(36)   NOT NULL,
  database_name         VARCHAR(128)  NOT NULL,
  table_name            VARCHAR(128)  NOT NULL,
  operation_type        VARCHAR(50)   NOT NULL,
  completed_at          TIMESTAMP(6)  NOT NULL,
  status                VARCHAR(20)   NOT NULL,
  orphan_files_deleted  BIGINT,
  orphan_bytes_deleted  BIGINT,
  error_message         VARCHAR(1024),
  error_type            VARCHAR(256),
  PRIMARY KEY (id),
  INDEX idx_toph_db_table (database_name, table_name),
  -- Drives TableOperationHistoryRepository.findLatestPerTable: the correlated
  -- MAX(completed_at) subquery becomes an index-only lookup per (operation_type,
  -- table_uuid) instead of an O(N²) scan.
  INDEX idx_toph_optype_uuid_completed (operation_type, table_uuid, completed_at)
);

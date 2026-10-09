CREATE TABLE table_operations (
  id             VARCHAR(36)   NOT NULL,
  table_uuid     VARCHAR(36)   NOT NULL,
  database_name  VARCHAR(128)  NOT NULL,
  table_name     VARCHAR(128)  NOT NULL,
  operation_type VARCHAR(50)   NOT NULL,
  status         VARCHAR(20)   NOT NULL,
  created_at     TIMESTAMP(6)  NOT NULL,
  scheduled_at   TIMESTAMP(6),
  job_id         VARCHAR(255),
  PRIMARY KEY (id),
  -- Localizes the commit-driven active-op lookup (AnalyzerRunner.loadCurrentOpsForTable) to one
  -- table's handful of rows, and answers "latest active op by type" for that table, instead of
  -- scanning the whole active queue on every commit.
  INDEX idx_to_table_uuid_optype (table_uuid, operation_type),
  -- Scheduler claim (find PENDING by type) and the daily full scan (filter by operation_type).
  INDEX idx_to_optype_status (operation_type, status)
);

CREATE TABLE table_stats (
  table_uuid       VARCHAR(36)   NOT NULL,
  database_name    VARCHAR(128)  NOT NULL,
  table_name       VARCHAR(128)  NOT NULL,
  snapshot         TEXT,
  table_properties TEXT,
  updated_at       TIMESTAMP(6)  NOT NULL,
  PRIMARY KEY (table_uuid),
  -- Backs findDistinctDatabaseNames (database_name leading) and per-database table_stats.find
  -- used by the analyzer full scan.
  INDEX idx_ts_db_table (database_name, table_name)
);

CREATE TABLE table_stats_history (
  id             VARCHAR(36)   NOT NULL,
  table_uuid     VARCHAR(36)   NOT NULL,
  database_name  VARCHAR(128)  NOT NULL,
  table_name     VARCHAR(128)  NOT NULL,
  snapshot       TEXT,
  delta          TEXT,
  recorded_at    TIMESTAMP(6)  NOT NULL,
  PRIMARY KEY (id),
  -- getStatsHistory: filter by table_uuid, optional recorded_at >= since, ordered by recorded_at.
  -- The composite serves the filter, range, and sort as an index-only scan.
  INDEX idx_tsh_table_uuid_recorded (table_uuid, recorded_at),
  -- Standalone recorded_at index for retention sweeps (delete rows older than the cutoff).
  INDEX idx_tsh_recorded_at (recorded_at)
);

CREATE TABLE table_operations_history (
  id             VARCHAR(36)   NOT NULL,
  table_uuid     VARCHAR(36)   NOT NULL,
  database_name  VARCHAR(128)  NOT NULL,
  table_name     VARCHAR(128)  NOT NULL,
  operation_type VARCHAR(50)   NOT NULL,
  completed_at   TIMESTAMP(6)  NOT NULL,
  status         VARCHAR(20)   NOT NULL,
  PRIMARY KEY (id),
  INDEX idx_toph_db_table (database_name, table_name),
  -- Commit-driven analyzer (loadLatestHistoryForTable): filter by table_uuid, ordered by
  -- completed_at. No existing index leads with table_uuid (the composite below leads with
  -- operation_type), so this query was a full scan without it.
  INDEX idx_toph_table_uuid_completed (table_uuid, completed_at),
  -- Drives TableOperationHistoryRepository.findLatestPerTable: the correlated
  -- MAX(completed_at) subquery becomes an index-only lookup per (operation_type,
  -- table_uuid) instead of an O(N²) scan.
  INDEX idx_toph_optype_uuid_completed (operation_type, table_uuid, completed_at)
);

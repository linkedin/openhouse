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

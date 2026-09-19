-- Baseline snapshot of the House Tables Service MySQL schema, recording its state
-- immediately before the entity_type column is added to user_table_row.
--
-- The service does not execute this file. Production DDL is applied out of band by the
-- MySQL/DDS team; this directory exists only so the sequence of schema changes is recorded
-- in the repository.
--
-- user_table_row and soft_deleted_user_table_row are verified against production
-- SHOW CREATE TABLE output, including secondary indexes, storage engine, character set
-- and collation, and physical column order. job_row and table_toggle_rule are still
-- derived from the service's bootstrap schema (src/main/resources/schema.sql) and remain
-- unverified, so treat them as an approximation until the MySQL team confirms them.
--
-- Written as bare CREATE TABLE rather than CREATE TABLE IF NOT EXISTS: this is a state
-- snapshot for reconstruction and audit, not an idempotent bootstrap command, and it must
-- never be run against the live database.

CREATE TABLE user_table_row (
                         database_id         VARCHAR (255)     NOT NULL,
                         table_id            VARCHAR (255)     NOT NULL,
                         metadata_location   VARCHAR (255)     ,
                         table_version       VARCHAR (255)     ,
                         version             BIGINT            DEFAULT NULL,
                         ETL_TS              DATETIME(6)       DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
                         deleted_ts          DATETIME(6)       DEFAULT NULL,
                         storage_type        VARCHAR (128)     DEFAULT 'hdfs' NOT NULL,
                         creation_time       BIGINT            DEFAULT NULL,
                         PRIMARY KEY (database_id, table_id),
                         KEY idx_user_table_upper_db_table ((upper(database_id)), (upper(table_id)), version, storage_type, creation_time)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE job_row (
    job_id                  VARCHAR (359)     NOT NULL,
    state                   VARCHAR (128)     NOT NULL,
    version                 BIGINT            ,
    job_name                VARCHAR (128)     NOT NULL,
    cluster_id              VARCHAR (128)      NOT NULL,
    creation_time_ms        BIGINT ,
    start_time_ms           BIGINT ,
    finish_time_ms          BIGINT ,
    last_update_time_ms     BIGINT ,
    job_conf                MEDIUMTEXT,
    heartbeat_time_ms       BIGINT ,
    execution_id            VARCHAR (128),
    engine_type             VARCHAR (128),
    ETL_TS                  DATETIME(6)      DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    retention_time_sec      BIGINT ,
    PRIMARY KEY (job_id)
    );

CREATE TABLE table_toggle_rule (
    feature                  VARCHAR (128)     NOT NULL,
    database_pattern         VARCHAR (128)     NOT NULL,
    table_pattern            VARCHAR (512)     NOT NULL,
    id                       BIGINT            AUTO_INCREMENT,
    creation_time_ms         BIGINT ,
    ETL_TS                   DATETIME(6)       DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE (feature, database_pattern, table_pattern)
    );

CREATE TABLE soft_deleted_user_table_row (
    database_id         VARCHAR (128)     NOT NULL,
    table_id            VARCHAR (128)     NOT NULL,
    deleted_at_ms       BIGINT            NOT NULL,
    version             BIGINT            NOT NULL,
    metadata_location   VARCHAR (512)     ,
    storage_type        VARCHAR (128)     DEFAULT 'hdfs' NOT NULL,
    creation_time       BIGINT            DEFAULT NULL,
    last_modified_time  TIMESTAMP         DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    ETL_TS              DATETIME(6)       DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    purge_after_ms      BIGINT          NOT NULL,
    PRIMARY KEY (database_id, table_id, deleted_at_ms)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

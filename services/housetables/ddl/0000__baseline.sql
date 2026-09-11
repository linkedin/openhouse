-- Baseline snapshot of the House Tables Service MySQL schema, recording its state
-- immediately before the entity_type column is added to user_table_row.
--
-- The service does not execute this file. Production DDL is applied out of band by the
-- MySQL/DDS team; this directory exists only so the sequence of schema changes is recorded
-- in the repository.
--
-- These definitions are derived from the service's bootstrap schema
-- (src/main/resources/schema.sql) and are PENDING VERIFICATION against production
-- SHOW CREATE TABLE output. A derived definition cannot capture secondary indexes,
-- storage engine, character set or collation, or physical column order that production
-- may have. Treat it as an approximation until the MySQL team confirms it.
--
-- Written as bare CREATE TABLE rather than CREATE TABLE IF NOT EXISTS: this is a state
-- snapshot for reconstruction and audit, not an idempotent bootstrap command, and it must
-- never be run against the live database.

CREATE TABLE user_table_row (
                         database_id         VARCHAR (128)     NOT NULL,
                         table_id            VARCHAR (128)     NOT NULL,
                         version             BIGINT            NOT NULL,
                         metadata_location   VARCHAR (512)     ,
                         storage_type        VARCHAR (128)     DEFAULT 'hdfs' NOT NULL,
                         creation_time       BIGINT            DEFAULT NULL,
                         last_modified_time  TIMESTAMP         DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                         ETL_TS              DATETIME(6)       DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
                         PRIMARY KEY (database_id, table_id)
);

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
);

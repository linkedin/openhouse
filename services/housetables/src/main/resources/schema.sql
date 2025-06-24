-- DDL in this file is in alpha stage and would be subject to change.
CREATE TABLE IF NOT EXISTS user_table_row (
                         database_id         VARCHAR (128)     NOT NULL,
                         table_id            VARCHAR (128)     NOT NULL,
                         version             BIGINT            NOT NULL,
                         metadata_location   VARCHAR (512)     ,
                         storage_type        VARCHAR (128)     DEFAULT 'hdfs' NOT NULL,
                         creation_time       BIGINT            DEFAULT NULL,
                         last_modified_time  TIMESTAMP         DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                         ETL_TS              DATETIME(6)       DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
                         deleted             BOOLEAN           DEFAULT FALSE,
                         PRIMARY KEY (database_id, table_id)
);

-- FIXME: Index is not added at this point.
-- FIXME: Types of timestamp column to be discussed.
CREATE TABLE IF NOT EXISTS job_row (
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
    ETL_TS                  datetime(6)      DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    retention_time_sec      BIGINT ,
    PRIMARY KEY (job_id)
    );

CREATE TABLE IF NOT EXISTS table_toggle_rule (
    feature                  VARCHAR (128)     NOT NULL,
    database_pattern         VARCHAR (128)     NOT NULL,
    table_pattern            VARCHAR (512)     NOT NULL,
    id                       BIGINT            AUTO_INCREMENT,
    creation_time_ms         BIGINT ,
    ETL_TS                   DATETIME(6)       DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (id),
    UNIQUE (feature, database_pattern, table_pattern)
    );
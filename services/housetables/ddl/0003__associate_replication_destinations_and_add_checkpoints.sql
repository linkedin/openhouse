-- replication_destination was introduced without replication associations and has no writers yet.
-- This migration assumes it is empty; do not infer source identity for any pre-existing row.
ALTER TABLE replication_destination
    CHANGE COLUMN database_id destination_database_id VARCHAR (128) NOT NULL,
    CHANGE COLUMN table_id destination_table_id VARCHAR (128) NOT NULL,
    ADD COLUMN source_cluster_id           VARCHAR (128) NOT NULL,
    ADD COLUMN source_table_uuid            VARCHAR (128) NOT NULL,
    ADD COLUMN source_creation_time         BIGINT        NOT NULL,
    ADD COLUMN source_database_id           VARCHAR (128) NOT NULL,
    ADD COLUMN source_table_id              VARCHAR (128) NOT NULL,
    ADD COLUMN destination_cluster_id       VARCHAR (128) NOT NULL,
    ADD COLUMN destination_table_uuid       VARCHAR (128) NOT NULL,
    ADD COLUMN destination_creation_time    BIGINT        NOT NULL,
    ADD COLUMN version                      BIGINT        NOT NULL DEFAULT 0,
    DROP PRIMARY KEY,
    ADD PRIMARY KEY (
        source_cluster_id,
        source_table_uuid,
        source_creation_time,
        destination_cluster_id,
        destination_table_uuid,
        destination_creation_time
    );

CREATE TABLE replication_checkpoint (
    source_cluster_id           VARCHAR (128) NOT NULL,
    source_table_uuid            VARCHAR (128) NOT NULL,
    source_creation_time         BIGINT        NOT NULL,
    destination_cluster_id       VARCHAR (128) NOT NULL,
    destination_table_uuid       VARCHAR (128) NOT NULL,
    destination_creation_time    BIGINT        NOT NULL,
    source_table_version         VARCHAR (512) DEFAULT NULL,
    source_snapshot_id           BIGINT        DEFAULT NULL,
    destination_snapshot_id      BIGINT        DEFAULT NULL,
    destination_table_version    VARCHAR (512) DEFAULT NULL,
    revision                     BIGINT        NOT NULL DEFAULT 0,
    ETL_TS                       DATETIME(6)   DEFAULT CURRENT_TIMESTAMP(6) ON UPDATE CURRENT_TIMESTAMP(6),
    PRIMARY KEY (
        source_cluster_id,
        source_table_uuid,
        source_creation_time,
        destination_cluster_id,
        destination_table_uuid,
        destination_creation_time
    )
);

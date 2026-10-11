-- Operations record for the destination-table identity schema. It contains no
-- table type, source association, or replication progress/checkpoint fields.
--
-- The service does not execute this file; production DDL is applied out of band
-- by the MySQL/DDS team.

CREATE TABLE replication_destination (
    database_id         VARCHAR (128)     NOT NULL,
    table_id            VARCHAR (128)     NOT NULL,
    PRIMARY KEY (database_id, table_id)
);

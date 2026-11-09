-- Apply this feature schema before serving database metadata requests.
-- Names are stored in lowercase for the same case-insensitive uniqueness in MySQL and H2.
-- Properties are a JSON document read and written with the versioned database row.

CREATE TABLE catalog_databases (
    database_uuid       VARCHAR (36)    NOT NULL,
    database_name       VARCHAR (128)   NOT NULL,
    properties          LONGTEXT        NOT NULL,
    version             BIGINT          NOT NULL,
    PRIMARY KEY (database_uuid),
    CONSTRAINT uq_database_name UNIQUE (database_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

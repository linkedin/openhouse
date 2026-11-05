-- This file records a schema change applied out of band by the MySQL/DDS team.
-- Deploy these tables before enabling database metadata requests.
-- The normalized name key enforces the same case-insensitive uniqueness in MySQL and H2.
-- Property keys are UTF-8 bytes, preserving case and trailing spaces across collations.

CREATE TABLE database_row (
    database_uuid       VARCHAR (36)    NOT NULL,
    database_name       VARCHAR (128)   NOT NULL,
    database_name_key   VARCHAR (128)   NOT NULL,
    version             BIGINT          NOT NULL,
    PRIMARY KEY (database_uuid),
    CONSTRAINT uq_database_name UNIQUE (database_name_key)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

CREATE TABLE database_property (
    database_uuid       VARCHAR (36)    NOT NULL,
    property_key        VARBINARY (2048) NOT NULL,
    property_value      MEDIUMTEXT      NOT NULL,
    PRIMARY KEY (database_uuid, property_key),
    CONSTRAINT fk_database_property_database
        FOREIGN KEY (database_uuid) REFERENCES database_row (database_uuid) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

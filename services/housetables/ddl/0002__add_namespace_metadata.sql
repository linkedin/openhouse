-- Apply this feature schema before serving namespace metadata requests.
-- Names retain insertion spelling; the column collation provides case-insensitive identity.
-- Properties are a JSON document read and written with the versioned namespace row.

CREATE TABLE catalog_namespaces (
    namespace_uuid      VARCHAR (36)    NOT NULL,
    namespace_name      VARCHAR (128)   NOT NULL,
    properties          LONGTEXT        NOT NULL,
    version             BIGINT          NOT NULL,
    PRIMARY KEY (namespace_uuid),
    CONSTRAINT uq_namespace_name UNIQUE (namespace_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

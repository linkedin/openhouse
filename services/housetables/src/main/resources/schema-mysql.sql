-- Namespace identity uses this collation independently of the database default.
CREATE TABLE IF NOT EXISTS catalog_namespaces (
    namespace_uuid      VARCHAR (36)    NOT NULL,
    namespace_name      VARCHAR (128)   NOT NULL,
    properties          LONGTEXT        NOT NULL,
    version             BIGINT          NOT NULL,
    PRIMARY KEY (namespace_uuid),
    CONSTRAINT uq_namespace_name UNIQUE (namespace_name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_0900_ai_ci;

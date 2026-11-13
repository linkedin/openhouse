-- Initialize namespace identity with H2's case-insensitive type before the shared schema.
CREATE TABLE IF NOT EXISTS catalog_namespaces (
    namespace_uuid      VARCHAR (36)    NOT NULL,
    namespace_name      VARCHAR_IGNORECASE (128) NOT NULL,
    properties          LONGTEXT        NOT NULL,
    version             BIGINT          NOT NULL,
    PRIMARY KEY (namespace_uuid),
    CONSTRAINT uq_namespace_name UNIQUE (namespace_name)
);

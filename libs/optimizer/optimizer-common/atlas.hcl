# Run Atlas from this directory; file:// paths are relative to it.
# Commands and rollout rules: docs/development/optimizer-schema-migrations.md.

# Explicit authoring: compare committed migrations with entity DDL, then write a candidate
# Flyway migration and atlas.sum. This can propose DROP statements; review the SQL before committing.
env "local" {
  # First run ./gradlew :services:optimizer:exportEntitySchema from the repository root.
  # Hibernate exports the mapped tables here without running migrations or requiring an existing DB.
  src = "file://../../../build/optimizer/entity-schema.sql"
  # Atlas uses disposable MySQL in Docker to compare the two schemas, never the application DB.
  # Keep the image aligned with MySqlContainerInitializer.MYSQL_IMAGE in the test fixtures.
  dev = "docker+mysql://_/mysql:8.0.46/dev"
  migration {
    dir = "file://src/main/resources/db/migration?format=flyway"
  }
}

# Build/CI verification (:services:optimizer:verifyMigrations): compare committed migrations with
# entity DDL without writing migration files or changing the application DB. Any reported diff fails.
# Ignore drops so tables, columns and indexes kept for older releases need not be mapped here.
# This does not prove write compatibility: extra NOT NULL columns or unique indexes can break writes.
env "verify" {
  dev = "docker+mysql://_/mysql:8.0.46/dev"
  diff {
    skip {
      drop_schema = true
      drop_table  = true
      drop_column = true
      drop_index  = true
    }
  }
}

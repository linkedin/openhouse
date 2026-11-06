# Atlas generates the Flyway migrations in src/main/resources/db/migration from the tables that the
# JPA entities define. Run it from this directory; see docs/development/optimizer-schema-migrations.md.
env "local" {
  # The entities' DDL, which Hibernate writes when the export task runs; it needs neither Docker nor
  # migrations that match the entities:
  # ./gradlew :services:optimizer:exportEntitySchema
  src = "file://../../../build/optimizer/entity-schema.sql"
  # Throwaway MySQL in Docker that Atlas replays the migrations and the entities' DDL on to diff
  # them. Keep it on the image the tests run on, MySqlContainerInitializer.MYSQL_IMAGE.
  dev = "docker+mysql://_/mysql:8.0.46/dev"
  migration {
    dir = "file://src/main/resources/db/migration?format=flyway"
  }
}

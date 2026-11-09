# Atlas drafts Flyway SQL explicitly; builds only verify committed migrations. A desired-state diff
# can propose drops, so review staged changes before committing; see the schema migration guide.
env "local" {
  # Export independently of tests, migrations and validation:
  # ./gradlew :services:optimizer:exportEntitySchema
  src = "file://../../../build/optimizer/entity-schema.sql"
  # Throwaway MySQL in Docker that Atlas replays the migrations and the entities' DDL on to diff
  # them. Keep it on the image the tests run on, MySqlContainerInitializer.MYSQL_IMAGE.
  dev = "docker+mysql://_/mysql:8.0.46/dev"
  migration {
    dir = "file://src/main/resources/db/migration?format=flyway"
  }
}

# Native verification is strict about mapped definitions, but retained rollout objects need not be
# removed just because the current entities no longer map them. This is not a write-safety check.
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

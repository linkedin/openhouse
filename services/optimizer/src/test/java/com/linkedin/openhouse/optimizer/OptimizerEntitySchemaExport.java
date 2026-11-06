package com.linkedin.openhouse.optimizer;

import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;

/**
 * Writes the DDL of the tables the JPA entities define, without a database that holds them:
 * Hibernate generates the script from the entity metadata at startup, on an in-memory H2 database
 * that is never migrated or validated. Run through the {@code exportEntitySchema} Gradle task,
 * which passes the output file as the only argument.
 */
public final class OptimizerEntitySchemaExport {

  private OptimizerEntitySchemaExport() {}

  public static void main(String[] args) {
    if (args.length != 1) {
      throw new IllegalArgumentException("Usage: OptimizerEntitySchemaExport <output-sql-file>");
    }
    new SpringApplicationBuilder(OptimizerServiceApplication.class)
        .web(WebApplicationType.NONE)
        .run(
            "--cluster.optimizer.database.url=jdbc:h2:mem:entity-schema;MODE=MYSQL;DB_CLOSE_DELAY=-1",
            "--cluster.optimizer.database.type=IN_MEMORY",
            "--cluster.optimizer.database.cert-based-auth.enabled=false",
            "--spring.datasource.username=sa",
            "--spring.datasource.password=",
            // The dialect stays MySQL8Dialect (application.properties) so the DDL is MySQL's.
            "--spring.flyway.enabled=false",
            "--spring.jpa.properties.javax.persistence.schema-generation.database.action=none",
            "--spring.jpa.properties.javax.persistence.schema-generation.scripts.action=create",
            "--spring.jpa.properties.javax.persistence.schema-generation.scripts.create-target="
                + args[0],
            "--spring.jpa.properties.hibernate.hbm2ddl.schema-generation.script.append=false",
            "--spring.jpa.properties.hibernate.hbm2ddl.delimiter=;")
        .close();
  }
}

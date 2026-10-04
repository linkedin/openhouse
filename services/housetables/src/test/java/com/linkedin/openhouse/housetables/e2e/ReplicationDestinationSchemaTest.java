package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
class ReplicationDestinationSchemaTest {

  private static final String DATABASE_ID = "replication_destination_schema_test";

  @Autowired DataSource dataSource;

  private JdbcTemplate jdbcTemplate() {
    return new JdbcTemplate(dataSource);
  }

  @AfterEach
  void cleanUp() {
    jdbcTemplate().update("DELETE FROM replication_destination WHERE database_id = ?", DATABASE_ID);
  }

  @Test
  void destinationIdentityIsTheCompositePrimaryKey() {
    JdbcTemplate jdbcTemplate = jdbcTemplate();
    jdbcTemplate.update(
        "INSERT INTO replication_destination (database_id, table_id) VALUES (?, ?)",
        DATABASE_ID,
        "destination_table");

    assertThatThrownBy(
            () ->
                jdbcTemplate.update(
                    "INSERT INTO replication_destination (database_id, table_id) VALUES (?, ?)",
                    DATABASE_ID,
                    "destination_table"))
        .isInstanceOf(DataIntegrityViolationException.class);
    assertThatThrownBy(
            () ->
                jdbcTemplate.update(
                    "INSERT INTO replication_destination (database_id, table_id) VALUES (?, ?)",
                    DATABASE_ID,
                    null))
        .isInstanceOf(DataIntegrityViolationException.class);
    assertThatThrownBy(
            () ->
                jdbcTemplate.update(
                    "INSERT INTO replication_destination (database_id, table_id) VALUES (?, ?)",
                    null,
                    "destination_table"))
        .isInstanceOf(DataIntegrityViolationException.class);

    assertThat(
            jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM replication_destination WHERE database_id = ?",
                Integer.class,
                DATABASE_ID))
        .isEqualTo(1);
  }
}

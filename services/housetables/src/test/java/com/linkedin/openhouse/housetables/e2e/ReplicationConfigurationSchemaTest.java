package com.linkedin.openhouse.housetables.e2e;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

@SpringBootTest(classes = SpringH2HtsApplication.class)
public class ReplicationConfigurationSchemaTest {

  private static final String INSERT_SQL =
      "INSERT INTO replication_configuration "
          + "(source_database_id, source_table_id, destination_cluster_id, "
          + "destination_database_id, destination_table_id, replication_interval, version) "
          + "VALUES (?, ?, ?, ?, ?, ?, ?)";

  @Autowired JdbcTemplate jdbcTemplate;

  @BeforeEach
  public void cleanTable() {
    jdbcTemplate.update("DELETE FROM replication_configuration");
  }

  @AfterEach
  public void tearDown() {
    jdbcTemplate.update("DELETE FROM replication_configuration");
  }

  @Test
  public void testBootstrapSchemaSupportsMaxLengthIdentifiersAndUniqueEdges() {
    String identifier = "x".repeat(128);
    Object[] row = {identifier, identifier, identifier, identifier, identifier, "12H", 1L};

    jdbcTemplate.update(INSERT_SQL, row);

    assertThat(
            jdbcTemplate.queryForObject(
                "SELECT ETL_TS IS NOT NULL FROM replication_configuration", Boolean.class))
        .isTrue();
    assertThatThrownBy(
            () ->
                jdbcTemplate.update(
                    INSERT_SQL,
                    identifier,
                    identifier,
                    identifier,
                    identifier,
                    identifier,
                    "1D",
                    1L))
        .isInstanceOf(DataIntegrityViolationException.class);
  }
}

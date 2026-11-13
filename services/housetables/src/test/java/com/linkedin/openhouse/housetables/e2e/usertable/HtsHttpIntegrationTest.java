package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.expectStatus;
import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.text;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient;
import com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.Response;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;

/**
 * Real HTTP against an embedded HTS on an allocated port, backed by the shared MySQL test store.
 * Each test maps to one scenario of the retired standalone HTS integration runner.
 */
@SpringBootTest(
    classes = SpringH2HtsApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class HtsHttpIntegrationTest {

  private static final String RUN_ID =
      Long.toString(System.currentTimeMillis(), 36)
          + "_"
          + UUID.randomUUID().toString().substring(0, 8);

  @LocalServerPort private int port;

  @Autowired private DataSource dataSource;

  private HtsHttpTestClient http;

  @BeforeEach
  public void connect() {
    http = HtsHttpTestClient.forPort(port);
  }

  private static String databaseId(String suffix) {
    return "hts_it_" + RUN_ID + "_" + suffix;
  }

  // ---------------------------------------------------------------------------------------------
  // the backend this suite claims to test
  // ---------------------------------------------------------------------------------------------

  /**
   * The service's own datasource must be the disposable MySQL database built from the canonical
   * DDL. The widths tell canonical DDL apart from the bootstrap schema.sql and Hibernate DDL.
   */
  @Test
  public void usesCanonicalMysqlSchema() throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      assertThat(connection.getMetaData().getDatabaseProductName())
          .as("the service datasource backend")
          .isEqualTo("MySQL");
    }
    JdbcTemplate jdbc = new JdbcTemplate(dataSource);
    assertThat(jdbc.queryForObject("SELECT DATABASE()", String.class)).isEqualTo("hts_test");

    Map<String, Map<String, Object>> columns =
        jdbc
            .queryForList(
                "SELECT column_name AS name, data_type AS type,"
                    + " character_maximum_length AS width, is_nullable AS nullable,"
                    + " collation_name AS collation FROM information_schema.columns"
                    + " WHERE table_schema = DATABASE() AND table_name = 'user_table_row'")
            .stream()
            .collect(
                Collectors.toMap(
                    row -> String.valueOf(row.get("name")).toLowerCase(Locale.ROOT), row -> row));
    assertColumn(columns, "entity_type", 128);
    assertThat(columns.get("entity_type").get("nullable")).isEqualTo("YES");
    assertThat(columns.get("entity_type").get("collation")).isEqualTo("utf8mb4_0900_ai_ci");
    for (String key : Arrays.asList("database_id", "table_id", "metadata_location")) {
      assertColumn(columns, key, 255);
    }

    List<String> keyExpressions =
        jdbc.queryForList(
            "SELECT expression FROM information_schema.statistics WHERE table_schema = DATABASE()"
                + " AND table_name = 'user_table_row'"
                + " AND index_name = 'idx_user_table_upper_db_table' ORDER BY seq_in_index",
            String.class);
    assertThat(keyExpressions).hasSizeGreaterThanOrEqualTo(2);
    assertThat(
            keyExpressions.subList(0, 2).stream()
                .map(expression -> expression.replace("`", "").toLowerCase(Locale.ROOT))
                .collect(Collectors.toList()))
        .as("the canonical functional index leads with the UPPER key expressions")
        .containsExactly("upper(database_id)", "upper(table_id)");
  }

  private static void assertColumn(
      Map<String, Map<String, Object>> columns, String name, long width) {
    Map<String, Object> column = columns.get(name);
    assertThat(column).as("user_table_row.%s", name).isNotNull();
    assertThat(column.get("type")).as("%s type", name).isEqualTo("varchar");
    assertThat(((Number) column.get("width")).longValue()).as("%s width", name).isEqualTo(width);
  }

  // ---------------------------------------------------------------------------------------------
  // 1-6: creation and payload/endpoint agreement
  // ---------------------------------------------------------------------------------------------

  @Test
  public void putTableDefaultsEntityTypeToTable() {
    String database = databaseId("put_table");
    Response response = http.putEntity("tables", database, "t1");
    expectStatus(response, 201, "PUT /hts/tables without entityType");
    JsonNode entity = response.entity();
    assertThat(text(entity, "entityType")).as(response.describe()).isEqualTo("TABLE");
    assertThat(text(entity, "databaseId")).as(response.describe()).isEqualTo(database);
    assertThat(text(entity, "tableId")).as(response.describe()).isEqualTo("t1");
  }

  // ---------------------------------------------------------------------------------------------
  // 24: request validation. HTS is unauthenticated, so a bad request is 400, not 401.
  // ---------------------------------------------------------------------------------------------

  @Test
  public void missingRequiredParametersAreBadRequest() {
    Map<String, Response> requests = new java.util.LinkedHashMap<>();
    requests.put("GET /hts/tables", http.get("/hts/tables", HtsHttpTestClient.params()));
    requests.put("GET /hts/views", http.get("/hts/views", HtsHttpTestClient.params()));
    requests.put("GET /hts/entities", http.get("/hts/entities", HtsHttpTestClient.params()));
    requests.put(
        "GET /hts/tables/querySoftDeleted",
        http.get("/hts/tables/querySoftDeleted", HtsHttpTestClient.params()));
    requests.put(
        "PATCH /hts/tables/rename",
        http.exchange(HttpMethod.PATCH, "/hts/tables/rename", HtsHttpTestClient.params(), null));
    requests.put(
        "DELETE /v1/hts/tables without isSoftDelete",
        http.exchange(
            HttpMethod.DELETE,
            "/v1/hts/tables",
            HtsHttpTestClient.params("databaseId", "d", "tableId", "t"),
            null));
    assertThat(requests).hasSize(6);
    requests.forEach((what, response) -> expectStatus(response, 400, what));
  }
}

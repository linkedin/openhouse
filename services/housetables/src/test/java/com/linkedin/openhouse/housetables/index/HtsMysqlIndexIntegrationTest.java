package com.linkedin.openhouse.housetables.index;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.housetables.e2e.SpringH2HtsApplication;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import javax.persistence.EntityManager;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;
import org.springframework.transaction.annotation.Transactional;
import org.testcontainers.containers.MySQLContainer;

@Tag("mysql-index")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@SpringBootTest(
    classes = SpringH2HtsApplication.class,
    properties = {
      "spring.sql.init.mode=never",
      "spring.jpa.open-in-view=false",
      "spring.jpa.properties.hibernate.jdbc.batch_size=0",
      "spring.jpa.properties.hibernate.cache.use_second_level_cache=false",
      "spring.jpa.properties.hibernate.cache.use_query_cache=false",
      "spring.datasource.hikari.maximum-pool-size=2"
    })
@Import(HtsMysqlIndexIntegrationTest.CaptureConfiguration.class)
@AutoConfigureMockMvc
@Transactional
class HtsMysqlIndexIntegrationTest {
  private static final ObjectMapper JSON = new ObjectMapper();
  private static final String DDL_REVISION = "970c87aaae748edb2bcafdb5d4f3e47495e91ac5";
  private static final String UPPER_INDEX = "idx_user_table_upper_db_table";
  private static final MySQLContainer<?> MYSQL =
      new MySQLContainer<>(System.getProperty("hts.index.mysqlImage", "mysql:8.4.11"))
          .withDatabaseName("hts_index_test")
          .withCommand(
              "--innodb-buffer-pool-size=134217728",
              "--character-set-server=utf8mb4",
              "--collation-server=utf8mb4_0900_ai_ci")
          .withCreateContainerCmdModifier(
              command ->
                  command
                      .getHostConfig()
                      .withMemory(512L * 1024 * 1024)
                      .withNanoCPUs(1_000_000_000L));

  @DynamicPropertySource
  static void mysqlProperties(DynamicPropertyRegistry properties) {
    MYSQL.start();
    properties.add("cluster.housetables.database.type", () -> "MYSQL");
    properties.add("cluster.housetables.database.url", MYSQL::getJdbcUrl);
    properties.add("HTS_DB_USER", MYSQL::getUsername);
    properties.add("HTS_DB_PASSWORD", MYSQL::getPassword);
  }

  @Autowired private MockMvc mvc;
  @Autowired private DataSource dataSource;
  @Autowired private EntityManager entityManager;
  private JdbcTemplate jdbc;

  @TestConfiguration
  static class CaptureConfiguration {
    @Bean
    static BeanPostProcessor explainDataSource() {
      return new BeanPostProcessor() {
        @Override
        public Object postProcessAfterInitialization(Object bean, String name) {
          return bean instanceof DataSource ? new ExplainingDataSource((DataSource) bean) : bean;
        }
      };
    }
  }

  @BeforeAll
  void seedAndVerifySchema() throws Exception {
    jdbc = new JdbcTemplate(dataSource);
    assertThat(jdbc.queryForObject("SELECT DATABASE()", String.class)).isEqualTo("hts_index_test");
    Map<String, String> ddlHashes = new LinkedHashMap<>();
    ddlHashes.put(
        "0000__baseline.sql", "19b23ce5077c1d16158ada5588209a5621402df0bdfd7f7d4b12e2b7dec061ff");
    ddlHashes.put(
        "0001__add_entity_type_to_user_table_row.sql",
        "f4b892447ba2941adf4be02bc336646d065cbb3cb7b437a97be095b5addbec6d");
    for (Map.Entry<String, String> ddl : ddlHashes.entrySet()) {
      org.springframework.core.io.ClassPathResource resource =
          new org.springframework.core.io.ClassPathResource("mysql-production/" + ddl.getKey());
      byte[] contents;
      try (java.io.InputStream input = resource.getInputStream()) {
        contents = input.readAllBytes();
      }
      String hash =
          String.format(
              Locale.ROOT,
              "%064x",
              new java.math.BigInteger(
                  1, java.security.MessageDigest.getInstance("SHA-256").digest(contents)));
      assertThat(hash)
          .as("Unmodified DDL from %s: %s", DDL_REVISION, ddl.getKey())
          .isEqualTo(ddl.getValue());
      new org.springframework.jdbc.datasource.init.ResourceDatabasePopulator(resource)
          .execute(dataSource);
    }
    List<Object[]> live = new ArrayList<>();
    List<Object[]> deleted = new ArrayList<>();
    List<Object[]> jobs = new ArrayList<>();
    List<Object[]> rules = new ArrayList<>();
    for (int i = 0; i < 10000; i++) {
      String db = String.format(Locale.ROOT, "Db%03d", i / 100);
      String table = String.format(Locale.ROOT, "Table%05d", i);
      live.add(
          new Object[] {
            db,
            table,
            1L,
            String.format(Locale.ROOT, "file:///index/%05d.json", i),
            "hdfs",
            HtsIndexScenarios.DELETED_AT
          });
      deleted.add(
          new Object[] {
            db,
            String.format(Locale.ROOT, "Table%05d", i / 10 * 10),
            HtsIndexScenarios.DELETED_AT + i % 10,
            1L,
            "file:///index/deleted.json",
            "hdfs",
            HtsIndexScenarios.DELETED_AT,
            HtsIndexScenarios.DELETED_AT + 1000 + i % 10
          });
      jobs.add(
          new Object[] {
            String.format(Locale.ROOT, "job%05d", i), "QUEUED", 1L, "index-test", "local"
          });
      rules.add(
          new Object[] {
            String.format(Locale.ROOT, "Feature%03d", i / 100),
            db,
            table,
            HtsIndexScenarios.DELETED_AT
          });
    }
    jdbc.batchUpdate(
        "INSERT INTO user_table_row"
            + " (database_id,table_id,version,metadata_location,storage_type,creation_time)"
            + " VALUES (?,?,?,?,?,?)",
        live);
    jdbc.batchUpdate(
        "INSERT INTO soft_deleted_user_table_row"
            + " (database_id,table_id,deleted_at_ms,version,metadata_location,storage_type,"
            + "creation_time,purge_after_ms) VALUES (?,?,?,?,?,?,?,?)",
        deleted);
    jdbc.batchUpdate(
        "INSERT INTO job_row (job_id,state,version,job_name,cluster_id)" + " VALUES (?,?,?,?,?)",
        jobs);
    jdbc.batchUpdate(
        "INSERT INTO table_toggle_rule"
            + " (feature,database_pattern,table_pattern,creation_time_ms) VALUES (?,?,?,?)",
        rules);
    Map<String, Object> fixture = new LinkedHashMap<>();
    fixture.put("ddlRevision", DDL_REVISION);
    fixture.put("ddlSha256", ddlHashes);
    fixture.put("mysqlVersion", jdbc.queryForObject("SELECT VERSION()", String.class));
    fixture.put("image", MYSQL.getDockerImageName());
    fixture.put("imageId", MYSQL.getContainerInfo().getImageId());
    fixture.put("memoryBytes", MYSQL.getContainerInfo().getHostConfig().getMemory());
    fixture.put("nanoCpus", MYSQL.getContainerInfo().getHostConfig().getNanoCPUs());
    fixture.put(
        "bufferPoolBytes", jdbc.queryForObject("SELECT @@innodb_buffer_pool_size", Long.class));
    fixture.put("collation", jdbc.queryForObject("SELECT @@collation_database", String.class));
    assertThat(fixture.get("memoryBytes")).isEqualTo(512L * 1024 * 1024);
    assertThat(fixture.get("nanoCpus")).isEqualTo(1_000_000_000L);
    assertThat(fixture.get("bufferPoolBytes")).isEqualTo(128L * 1024 * 1024);
    assertThat(fixture.get("collation")).isEqualTo("utf8mb4_0900_ai_ci");
    for (String table :
        Arrays.asList(
            "user_table_row", "soft_deleted_user_table_row", "job_row", "table_toggle_rule")) {
      assertThat(jdbc.queryForObject("SELECT COUNT(*) FROM " + table, Integer.class))
          .isEqualTo(10000);
      jdbc.execute("ANALYZE TABLE " + table);
      fixture.put(table, jdbc.queryForList("SHOW CREATE TABLE " + table));
    }
    List<Map<String, Object>> indexes =
        jdbc.queryForList(
            "SELECT TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX,NON_UNIQUE,COLUMN_NAME,EXPRESSION FROM"
                + " information_schema.statistics WHERE TABLE_SCHEMA=DATABASE()"
                + " ORDER BY TABLE_NAME,INDEX_NAME,SEQ_IN_INDEX");
    fixture.put("indexes", indexes);
    assertThat(
            indexes.stream()
                .filter(row -> row.get("EXPRESSION") != null)
                .map(row -> row.get("EXPRESSION").toString())
                .collect(java.util.stream.Collectors.toList()))
        .containsExactlyInAnyOrder("upper(`database_id`)", "upper(`table_id`)");
    List<Map<String, Object>> upperIndex =
        indexes.stream()
            .filter(
                row ->
                    "user_table_row".equals(row.get("TABLE_NAME"))
                        && UPPER_INDEX.equals(row.get("INDEX_NAME")))
            .collect(java.util.stream.Collectors.toList());
    assertThat(upperIndex)
        .hasSize(5)
        .allSatisfy(row -> assertThat(((Number) row.get("NON_UNIQUE")).intValue()).isEqualTo(1));
    assertThat(
            upperIndex.stream()
                .map(
                    row ->
                        row.get("EXPRESSION") == null
                            ? row.get("COLUMN_NAME")
                            : row.get("EXPRESSION"))
                .collect(java.util.stream.Collectors.toList()))
        .containsExactly(
            "upper(`database_id`)",
            "upper(`table_id`)",
            "version",
            "storage_type",
            "creation_time");
    List<Map<String, Object>> deletedIndexes =
        indexes.stream()
            .filter(row -> "soft_deleted_user_table_row".equals(row.get("TABLE_NAME")))
            .collect(java.util.stream.Collectors.toList());
    assertThat(deletedIndexes).hasSize(3);
    assertThat(deletedIndexes)
        .allSatisfy(
            row -> {
              assertThat(row.get("INDEX_NAME")).isEqualTo("PRIMARY");
              assertThat(row.get("EXPRESSION")).isNull();
            });
    assertThat(
            deletedIndexes.stream()
                .map(row -> row.get("COLUMN_NAME"))
                .collect(java.util.stream.Collectors.toList()))
        .containsExactly("database_id", "table_id", "deleted_at_ms");
    writeReport("fixture", fixture);
  }

  static Stream<HtsIndexScenarios> endpoints() {
    return HtsIndexScenarios.all().stream()
        .filter(scenario -> !scenario.tables.contains("soft_deleted_user_table_row"))
        .filter(scenario -> !scenario.name.equals("jobs-query-id"));
  }

  static Stream<HtsIndexScenarios> softDeleteEndpoints() {
    return HtsIndexScenarios.all().stream()
        .filter(scenario -> scenario.tables.contains("soft_deleted_user_table_row"));
  }

  static Stream<HtsIndexScenarios> jobIdQueryEndpoints() {
    return HtsIndexScenarios.all().stream()
        .filter(scenario -> scenario.name.equals("jobs-query-id"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("endpoints")
  void endpointUsesSelectiveAccessWhenKeysAreProvided(HtsIndexScenarios scenario) throws Exception {
    verifyEndpoint(scenario);
  }

  // TODO: Fix soft-delete access patterns to use the plain composite primary key, then re-enable.
  @Disabled("Pending soft-delete primary-key access-pattern fix")
  @ParameterizedTest(name = "{0}")
  @MethodSource("softDeleteEndpoints")
  void softDeleteEndpointUsesPrimaryKey(HtsIndexScenarios scenario) throws Exception {
    verifyEndpoint(scenario);
  }

  // TODO: Fix job-ID query access patterns to avoid enumerating job_row, then re-enable.
  @Disabled("Pending indexed job-ID query access-pattern fix")
  @ParameterizedTest(name = "{0}")
  @MethodSource("jobIdQueryEndpoints")
  void jobIdQueryUsesPrimaryKey(HtsIndexScenarios scenario) throws Exception {
    verifyEndpoint(scenario);
  }

  private void verifyEndpoint(HtsIndexScenarios scenario) throws Exception {
    if (scenario.views) {
      jdbc.update(
          "UPDATE user_table_row SET entity_type='VIEW' WHERE database_id=?", HtsIndexScenarios.DB);
    }
    if (scenario.restore) {
      jdbc.update(
          "DELETE FROM user_table_row WHERE database_id=? AND table_id=?",
          HtsIndexScenarios.DB,
          HtsIndexScenarios.TABLE);
    }
    entityManager.clear();
    List<ExplainingDataSource.Plan> plans;
    MvcResult result;
    ExplainingDataSource.begin();
    try {
      result = mvc.perform(scenario.request()).andReturn();
      entityManager.flush();
    } finally {
      plans = ExplainingDataSource.end();
      Map<String, Object> evidence = new LinkedHashMap<>();
      evidence.put("endpoint", scenario.route());
      evidence.put("parameters", scenario.parameters);
      evidence.put("maxRowsPerAccess", scenario.rowBudget);
      evidence.put("permittedScanReason", scenario.scanReason);
      evidence.put("plans", plans);
      writeReport(scenario.name, evidence);
    }
    assertThat(result.getResponse().getStatus())
        .as("%s response: %s", scenario, result.getResponse().getContentAsString())
        .isEqualTo(scenario.status);
    assertThat(plans).as("%s must execute real SQL (no mocks/cache)", scenario).isNotEmpty();
    for (String table : scenario.tables) {
      assertThat(plans.stream().anyMatch(plan -> plan.sql.contains(table)))
          .as("%s must reach %s", scenario, table)
          .isTrue();
    }
    if (scenario.mutation != null) {
      assertThat(
              plans.stream()
                  .anyMatch(
                      plan ->
                          plan.sql.toLowerCase(Locale.ROOT).startsWith(scenario.mutation + " ")))
          .as("%s must inspect the mutation as well as its read-before-write", scenario)
          .isTrue();
    }
    if (scenario.countRequired) {
      assertThat(
              plans.stream().anyMatch(plan -> plan.sql.toLowerCase(Locale.ROOT).contains("count(")))
          .as("%s must force the paginated count query", scenario)
          .isTrue();
    }
    if (scenario.expectedResults != null) {
      JsonNode body = JSON.readTree(result.getResponse().getContentAsString());
      JsonNode rows =
          body.has("results") && body.get("results").isArray()
              ? body.get("results")
              : body.path("pageResults").path("content");
      assertThat(rows.isArray()).as("Response contains a result array: %s", body).isTrue();
      assertThat(rows.size()).isEqualTo(scenario.expectedResults);
    }
    if (scenario.rowBudget > 0) {
      List<String> failures = new ArrayList<>();
      for (ExplainingDataSource.Plan plan : plans) {
        for (String failure : MysqlIndexPlan.violations(plan.explain, scenario.rowBudget)) {
          failures.add(failure + "\nSQL: " + plan.sql + "\nBindings: " + plan.bindings);
        }
      }
      assertThat(failures)
          .as("%s index contract; full plans in %s", scenario, reportDirectory())
          .isEmpty();
    }
  }

  @Test
  void detectorRejectsLowerAndMissingIndexButAcceptsTheMatchingUpperLookup() throws Exception {
    String sql =
        "SELECT metadata_location FROM user_table_row"
            + " WHERE %s(database_id)=%s(?) AND %s(table_id)=%s(?)";
    JsonNode upper = explain(String.format(sql, "upper", "upper", "upper", "upper"));
    JsonNode lower = explain(String.format(sql, "lower", "lower", "lower", "lower"));
    JsonNode missing =
        explain(
            "SELECT metadata_location FROM user_table_row IGNORE INDEX ("
                + UPPER_INDEX
                + ")"
                + " WHERE upper(database_id)=upper(?) AND upper(table_id)=upper(?)");
    assertThat(MysqlIndexPlan.violations(upper, 10)).isEmpty();
    assertThat(MysqlIndexPlan.tables(upper).get(0).path("key").asText()).isEqualTo(UPPER_INDEX);
    assertThat(MysqlIndexPlan.violations(lower, 10)).isNotEmpty();
    assertThat(MysqlIndexPlan.violations(missing, 10)).isNotEmpty();
    writeReport(
        "detector-controls", Map.of("upper", upper, "lower", lower, "missingIndex", missing));
  }

  private JsonNode explain(String sql) throws Exception {
    try (Connection connection = dataSource.getConnection();
        PreparedStatement statement = connection.prepareStatement("EXPLAIN FORMAT=JSON " + sql)) {
      statement.setString(1, HtsIndexScenarios.DB.toLowerCase(Locale.ROOT));
      statement.setString(2, HtsIndexScenarios.TABLE.toUpperCase(Locale.ROOT));
      try (ResultSet result = statement.executeQuery()) {
        assertThat(result.next()).isTrue();
        return JSON.readTree(result.getString(1));
      }
    }
  }

  @Test
  void softDeletePrimaryKeySupportsBareKeysNotCaseFunctions() throws Exception {
    Map<String, Object> controls = new LinkedHashMap<>();
    for (String operation : Arrays.asList("SELECT metadata_location FROM", "DELETE FROM")) {
      String bare =
          operation + " soft_deleted_user_table_row" + " WHERE database_id=? AND table_id=?";
      JsonNode indexed = explain(bare);
      assertThat(MysqlIndexPlan.violations(indexed, 30)).isEmpty();
      assertThat(MysqlIndexPlan.tables(indexed).get(0).path("key").asText()).isEqualTo("PRIMARY");
      controls.put(operation + "-bare", indexed);
      for (String function : Arrays.asList("lower", "upper")) {
        JsonNode wrapped =
            explain(
                operation
                    + " soft_deleted_user_table_row WHERE "
                    + function
                    + "(database_id)="
                    + function
                    + "(?) AND "
                    + function
                    + "(table_id)="
                    + function
                    + "(?)");
        assertThat(MysqlIndexPlan.violations(wrapped, 30))
            .as("%s cannot use the plain soft-delete primary key: %s", operation, function)
            .isNotEmpty();
        controls.put(operation + "-" + function, wrapped);
      }
    }
    // CI collation, not a functional index, supplies case-insensitive matching.
    assertThat(
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM soft_deleted_user_table_row WHERE database_id=? AND table_id=?",
                Integer.class,
                HtsIndexScenarios.DB.toLowerCase(Locale.ROOT),
                HtsIndexScenarios.TABLE.toUpperCase(Locale.ROOT)))
        .isEqualTo(10);
    writeReport("soft-delete-primary-key-controls", controls);
  }

  private static Path reportDirectory() {
    return Paths.get(System.getProperty("hts.index.reportDir", "build/reports/mysql-index-plans"));
  }

  private static void writeReport(String name, Object evidence) throws Exception {
    Files.createDirectories(reportDirectory());
    JSON.writerWithDefaultPrettyPrinter()
        .writeValue(
            reportDirectory().resolve(name.replaceAll("[^a-zA-Z0-9-]", "_") + ".json").toFile(),
            evidence);
  }

  @AfterAll
  static void stopMysql() {
    MYSQL.stop();
  }
}

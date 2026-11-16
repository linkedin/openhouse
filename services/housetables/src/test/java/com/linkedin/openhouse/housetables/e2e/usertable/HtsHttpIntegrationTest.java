package com.linkedin.openhouse.housetables.e2e.usertable;

import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.INITIAL_VERSION;
import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.defaultLocation;
import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.expectMessage;
import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.expectStatus;
import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.params;
import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.tableIds;
import static com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.text;
import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.common.test.cluster.PropertyOverrideContextInitializer;
import com.linkedin.openhouse.housetables.e2e.HtsDatabaseTestBase;
import com.linkedin.openhouse.housetables.e2e.SpringHtsTestApplication;
import com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient;
import com.linkedin.openhouse.housetables.e2e.fixture.HtsHttpTestClient.Response;
import com.linkedin.openhouse.housetables.e2e.fixture.UserTableRawSeeder;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.server.LocalServerPort;
import org.springframework.http.HttpMethod;
import org.springframework.jdbc.core.JdbcOperations;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;
import org.springframework.test.context.ContextConfiguration;

/**
 * Real HTTP against an embedded HTS on an allocated port, backed by the shared MySQL test store and
 * the real controllers, services, repositories and Hikari pool. Nothing on this path is mocked.
 *
 * <p>Each numbered test is the JUnit counterpart of one scenario of the standalone Python HTS
 * integration runner. Healthy rows are written over HTTP; raw SQL is reserved for stored states no
 * request can produce (a NULL or corrupt discriminator) and for reading the column itself, because
 * a TABLE response cannot distinguish a stored {@code 'TABLE'} from a stored NULL.
 */
@SpringBootTest(
    classes = SpringHtsTestApplication.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ContextConfiguration(initializers = PropertyOverrideContextInitializer.class)
public class HtsHttpIntegrationTest extends HtsDatabaseTestBase {

  private static final String RUN_ID =
      Long.toString(System.currentTimeMillis(), 36)
          + "_"
          + UUID.randomUUID().toString().substring(0, 8);

  private static final String COLUMN_COLLATION = "utf8mb4_0900_ai_ci";

  @LocalServerPort private int port;

  @Autowired private DataSource dataSource;

  @Autowired private UserTableRawSeeder userTableRawSeeder;

  private HtsHttpTestClient http;

  private JdbcTemplate jdbc;

  @BeforeEach
  public void connect() {
    http = HtsHttpTestClient.forPort(port);
    jdbc = new JdbcTemplate(dataSource);
  }

  /** Unique per test and short enough for the archive's 128-character key columns. */
  private static String databaseId(String suffix) {
    return "hts_it_" + RUN_ID + "_" + suffix;
  }

  private enum Existence {
    EXISTS,
    ABSENT,
    ERROR
  }

  /** 200 exists, 404 absent; anything else is an error, never folded into either. */
  private static Existence existence(Response response) {
    switch (response.status()) {
      case 200:
        return Existence.EXISTS;
      case 404:
        return Existence.ABSENT;
      default:
        return Existence.ERROR;
    }
  }

  private static void expectEntityType(Response response, String expected) {
    assertThat(text(response.entity(), "entityType")).as(response.describe()).isEqualTo(expected);
  }

  private static void expectCorruptValueDiagnostic(Response response, String stored, String what) {
    assertThat(response.message())
        .as(
            "%s: the diagnostic must name the column and quote the value. %s",
            what, response.describe())
        .contains("user_table_row.entity_type")
        .contains("'" + stored + "'");
  }

  private UserTableRow rawRow(String database, String table) {
    return UserTableRow.builder()
        .databaseId(database)
        .tableId(table)
        .metadataLocation("/tmp/legacy.json")
        .storageType("hdfs")
        .build();
  }

  /** The stored discriminator and whether it is SQL NULL, which no API read can tell apart. */
  private Map<String, Object> readEntityTypeColumn(String database, String table) {
    return jdbc.queryForMap(
        "SELECT entity_type AS stored_value, entity_type IS NULL AS is_null FROM user_table_row"
            + " WHERE database_id = ? AND table_id = ?",
        database,
        table);
  }

  private static boolean isNull(Map<String, Object> column) {
    return ((Number) column.get("is_null")).intValue() == 1;
  }

  private long countLiveRows(String database, String table) {
    return jdbc.queryForObject(
        "SELECT COUNT(*) FROM user_table_row WHERE database_id = ? AND table_id = ?",
        Long.class,
        database,
        table);
  }

  // ---------------------------------------------------------------------------------------------
  // the backend this suite claims to test
  // ---------------------------------------------------------------------------------------------

  /**
   * The service's own datasource must be the selected disposable database built from the canonical
   * DDL. The widths tell canonical DDL apart from the bootstrap schema.sql and Hibernate DDL.
   */
  @Test
  public void usesCanonicalMysqlSchema() throws SQLException {
    try (Connection connection = dataSource.getConnection()) {
      assertSelectedBackend(connection);
    }
    assertThat(jdbc.queryForObject("SELECT DATABASE()", String.class)).isEqualTo(DATABASE);

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
    assertThat(columns.get("entity_type").get("collation")).isEqualTo(COLUMN_COLLATION);
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

  /** Scenario 1. */
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

  /** Scenario 2. */
  @Test
  public void putViewSetsEntityTypeView() {
    Response response = http.putEntity("views", databaseId("put_view"), "v1");
    expectStatus(response, 201, "PUT /hts/views without entityType");
    expectEntityType(response, "VIEW");
  }

  /** Scenario 3. */
  @Test
  public void putLowercaseEntityTypeIsNormalized() {
    Response response =
        http.putEntity("views", databaseId("lowercase_type"), "v1", INITIAL_VERSION, null, "view");
    expectStatus(response, 201, "PUT /hts/views with entityType 'view'");
    expectEntityType(response, "VIEW");
  }

  /** Scenario 4. */
  @Test
  public void putUnknownEntityTypeIsRejected() {
    String database = databaseId("garbage_type");
    Response response = http.putEntity("tables", database, "t1", INITIAL_VERSION, null, "GARBAGE");
    expectStatus(response, 400, "PUT /hts/tables with entityType GARBAGE");
    expectMessage(
        response,
        "entityType provided: GARBAGE, but this endpoint serves TABLE only",
        "PUT /hts/tables with entityType GARBAGE");
    expectStatus(
        http.getEntity("entities", database, "t1"), 404, "a rejected PUT must not create a row");
  }

  /** Scenario 5: a payload may agree with its route or stay silent, never override it. */
  @Test
  public void putMismatchedEntityTypeIsRejected() {
    String database = databaseId("mismatched_type");
    Response response = http.putEntity("tables", database, "t1", INITIAL_VERSION, null, "VIEW");
    expectStatus(response, 400, "PUT /hts/tables with entityType VIEW");
    expectMessage(
        response,
        "entityType provided: VIEW, but this endpoint serves TABLE only",
        "PUT /hts/tables with entityType VIEW");

    response = http.putEntity("views", database, "v1", INITIAL_VERSION, null, "TABLE");
    expectStatus(response, 400, "PUT /hts/views with entityType TABLE");
    expectMessage(
        response,
        "entityType provided: TABLE, but this endpoint serves VIEW only",
        "PUT /hts/views with entityType TABLE");
  }

  /** Scenario 6: the key is shared between the two types, so the wrong type is a collision. */
  @Test
  public void putConflictingEntityTypeAtOccupiedKey() {
    String database = databaseId("type_collision");
    JsonNode view = http.createView(database, "shared");

    Response response = http.putEntity("tables", database, "shared");
    expectStatus(response, 409, "PUT /hts/tables over an existing view");
    expectMessage(
        response, "VIEW " + database + ".shared already exists", "PUT /hts/tables over a view");

    Response survivor = http.getEntity("views", database, "shared");
    expectStatus(survivor, 200, "the view must survive a rejected table create");
    expectEntityType(survivor, "VIEW");
    assertThat(text(survivor.entity(), "metadataLocation"))
        .as("the occupant must be unmodified. %s", survivor.describe())
        .isEqualTo(text(view, "metadataLocation"));
  }

  // ---------------------------------------------------------------------------------------------
  // 7-12: reads. /hts/tables and /hts/views are type scoped, /hts/entities is not
  // ---------------------------------------------------------------------------------------------

  /** Scenario 7. */
  @Test
  public void getIsTypeScoped() {
    String database = databaseId("get_scoped");
    http.createTable(database, "t_tbl");
    http.createView(database, "v_vw");

    expectStatus(http.getEntity("tables", database, "v_vw"), 404, "GET /hts/tables on a view");

    Response response = http.getEntity("views", database, "t_tbl");
    expectStatus(response, 404, "GET /hts/views on a table");
    expectMessage(
        response, "View " + database + ".t_tbl cannot be found", "GET /hts/views on a table");

    response = http.getEntity("tables", database, "t_tbl");
    expectStatus(response, 200, "GET /hts/tables on a table");
    expectEntityType(response, "TABLE");

    response = http.getEntity("views", database, "v_vw");
    expectStatus(response, 200, "GET /hts/views on a view");
    expectEntityType(response, "VIEW");
  }

  /** Scenario 8. */
  @Test
  public void getEntitiesResolvesEitherType() {
    String database = databaseId("entities");
    http.createTable(database, "t_tbl");
    http.createView(database, "v_vw");

    Response response = http.getEntity("entities", database, "t_tbl");
    expectStatus(response, 200, "GET /hts/entities on a table");
    expectEntityType(response, "TABLE");

    response = http.getEntity("entities", database, "v_vw");
    expectStatus(response, 200, "GET /hts/entities on a view");
    expectEntityType(response, "VIEW");

    response = http.getEntity("entities", database, "absent");
    expectStatus(response, 404, "GET /hts/entities on a free key");
    expectMessage(
        response, "Entity " + database + ".absent cannot be found", "GET /hts/entities free key");
  }

  /**
   * Scenario 9: for well-formed rows the neutral and the matching typed read reach the same, true
   * verdict. Corrupt rows break this deliberately and are characterized in scenario 23.
   */
  @Test
  public void neutralAndTypedEndpointsAgreeOnExistence() {
    String database = databaseId("agreement");
    http.createTable(database, "t_tbl");
    http.createView(database, "v_vw");

    assertAgreement(database, "t_tbl", "tables", Existence.EXISTS, "a healthy TABLE");
    assertAgreement(database, "v_vw", "views", Existence.EXISTS, "a healthy VIEW");
    assertAgreement(database, "absent", "tables", Existence.ABSENT, "a never-written key");
  }

  private void assertAgreement(
      String database, String table, String kind, Existence expected, String what) {
    Response neutral = http.getEntity("entities", database, table);
    Response typed = http.getEntity(kind, database, table);
    assertThat(Arrays.asList(existence(neutral), existence(typed)))
        .as(
            "%s: /hts/entities vs /hts/%s. %s || %s",
            what, kind, neutral.describe(), typed.describe())
        .containsExactly(expected, expected);
  }

  /** Scenario 10. */
  @Test
  public void queryEndpointsAreTypeScoped() {
    String database = databaseId("query_scoped");
    http.createTable(database, "t_tbl");
    http.createView(database, "v_vw");

    List<JsonNode> tables = http.queryEntities("tables", database);
    assertThat(tableIds(tables)).containsExactly("t_tbl");
    assertThat(tables)
        .allSatisfy(entity -> assertThat(text(entity, "entityType")).isEqualTo("TABLE"));

    List<JsonNode> views = http.queryEntities("views", database);
    assertThat(tableIds(views)).containsExactly("v_vw");
    assertThat(views)
        .allSatisfy(entity -> assertThat(text(entity, "entityType")).isEqualTo("VIEW"));
  }

  /**
   * Scenario 11: the versioned queries answer an explicit null {@code results} and a page; the
   * legacy table query answers the reverse. The client helpers enforce both envelopes.
   */
  @Test
  public void v1QueryReturnsPageResults() {
    String database = databaseId("paginated");
    http.createTable(database, "t_tbl");
    http.createView(database, "v_vw");

    assertThat(tableIds(http.queryEntitiesPaginated("views", database))).containsExactly("v_vw");
    assertThat(tableIds(http.queryEntitiesPaginated("tables", database))).containsExactly("t_tbl");
    assertThat(tableIds(http.queryEntities("tables", database))).containsExactly("t_tbl");
  }

  /** Scenario 12: listing and cleanup must both reach past the first page. */
  @Test
  public void viewPaginationAndCleanupCoverMultiplePages() {
    String database = databaseId("view_pages");
    Set<String> expected =
        IntStream.range(0, 51)
            .mapToObj(number -> String.format(Locale.ROOT, "v_%03d", number))
            .collect(Collectors.toCollection(TreeSet::new));
    for (String view : expected) {
      http.createView(database, view);
    }

    List<JsonNode> views = http.queryEntities("views", database);
    assertThat(views).as("every page, no duplicates").hasSize(expected.size());
    assertThat(tableIds(views)).isEqualTo(expected);

    http.cleanup(database);

    assertThat(http.queryEntities("views", database))
        .as("cleanup must delete views beyond the first page")
        .isEmpty();
    assertThat(
            jdbc.queryForObject(
                "SELECT COUNT(*) FROM user_table_row WHERE database_id = ?", Long.class, database))
        .as("no live rows remain in this database after HTTP cleanup")
        .isZero();
  }

  // ---------------------------------------------------------------------------------------------
  // 13: updates. tableVersion is a compare-and-swap token on metadataLocation
  // ---------------------------------------------------------------------------------------------

  /** Scenario 13. */
  @Test
  public void updateRequiresCurrentMetadataLocation() {
    String database = databaseId("cas");
    String current = text(http.createTable(database, "t1"), "metadataLocation");

    Response response = http.putEntity("tables", database, "t1", current, current + "_v2", null);
    expectStatus(response, 200, "PUT with the current metadataLocation as tableVersion");
    assertThat(text(response.entity(), "metadataLocation"))
        .as(response.describe())
        .isEqualTo(current + "_v2");

    // The token is now stale: it names the location the update just replaced.
    expectStatus(
        http.putEntity("tables", database, "t1", current, current + "_v3", null),
        409,
        "PUT with a stale tableVersion");
    expectStatus(
        http.putEntity("tables", database, "t1"), 409, "PUT with INITIAL_VERSION over a row");

    Response stored = http.getEntity("tables", database, "t1");
    expectStatus(stored, 200, "reading the updated table");
    assertThat(text(stored.entity(), "metadataLocation"))
        .as("a rejected update must not overwrite the successful one. %s", stored.describe())
        .isEqualTo(current + "_v2");
  }

  // ---------------------------------------------------------------------------------------------
  // 14-15: deletes
  // ---------------------------------------------------------------------------------------------

  /** Scenario 14. */
  @Test
  public void deleteTableIsTableScoped() {
    String database = databaseId("delete_table");
    http.createView(database, "v_vw");

    expectStatus(
        http.deleteEntity("tables", database, "v_vw"), 404, "DELETE /hts/tables on a view");
    expectStatus(http.getEntity("views", database, "v_vw"), 200, "the view must survive");
  }

  /** Scenario 15. */
  @Test
  public void deleteViewRemovesOnlyTheView() {
    String database = databaseId("delete_view");
    http.createTable(database, "sibling");
    http.createView(database, "v_vw");

    Response response = http.deleteEntity("views", database, "v_vw");
    expectStatus(response, 204, "DELETE /hts/views on a view");
    assertThat(response.body()).as("204 must carry no body").isEmpty();

    expectStatus(http.getEntity("views", database, "v_vw"), 404, "the view must be gone");
    expectStatus(http.getEntity("tables", database, "sibling"), 200, "the sibling is untouched");
    expectStatus(
        http.deleteEntity("views", database, "v_vw"), 404, "DELETE of an already deleted view");
  }

  // ---------------------------------------------------------------------------------------------
  // 16-18: rename, PATCH /hts/tables/rename
  // ---------------------------------------------------------------------------------------------

  /** Scenario 16. */
  @Test
  public void renameTableSucceeds() {
    String database = databaseId("rename_ok");
    http.createTable(database, "before");

    String location = defaultLocation(database, "after");
    expectStatus(
        http.renameTable(database, "before", "after", location), 204, "PATCH rename on a table");

    expectStatus(http.getEntity("tables", database, "before"), 404, "the source key is free");
    Response renamed = http.getEntity("tables", database, "after");
    expectStatus(renamed, 200, "the destination holds the renamed table");
    assertThat(text(renamed.entity(), "metadataLocation"))
        .as(renamed.describe())
        .isEqualTo(location);
    expectEntityType(renamed, "TABLE");
  }

  /** Scenario 17: renaming a view through the table endpoint matches zero rows. */
  @Test
  public void renameIsTableScoped() {
    String database = databaseId("rename_scoped");
    http.createView(database, "v_vw");

    Response response = http.renameTable(database, "v_vw", "v_renamed", null);
    expectStatus(response, 404, "PATCH /hts/tables/rename on a view");
    expectMessage(
        response, "User table " + database + ".v_vw cannot be found", "PATCH rename on a view");

    expectStatus(http.getEntity("views", database, "v_vw"), 200, "the view must survive");
    expectStatus(
        http.getEntity("entities", database, "v_renamed"), 404, "no destination is created");
  }

  /** Scenario 18. */
  @Test
  public void renameOntoOccupiedDestinationConflicts() {
    String database = databaseId("rename_conflict");
    http.createTable(database, "src");
    JsonNode occupiedTable = http.createTable(database, "occupied_tbl");
    JsonNode occupiedView = http.createView(database, "occupied_vw");

    Response response = http.renameTable(database, "src", "occupied_tbl", null);
    expectStatus(response, 409, "rename onto a key held by a table");
    expectMessage(response, "Table occupied_tbl already exists", "rename onto a table");

    response = http.renameTable(database, "src", "occupied_vw", null);
    expectStatus(response, 409, "rename onto a key held by a view");
    expectMessage(response, "Table occupied_vw already exists", "rename onto a view");

    expectStatus(http.getEntity("tables", database, "src"), 200, "the source stays in place");
    for (JsonNode occupant : Arrays.asList(occupiedTable, occupiedView)) {
      Response current = http.getEntity("entities", database, text(occupant, "tableId"));
      expectStatus(current, 200, "a rejected rename leaves the occupant in place");
      assertThat(text(current.entity(), "entityType")).isEqualTo(text(occupant, "entityType"));
      assertThat(text(current.entity(), "metadataLocation"))
          .as("the occupant's metadataLocation must not be overwritten. %s", current.describe())
          .isEqualTo(text(occupant, "metadataLocation"));
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 19-21: soft delete lifecycle
  // ---------------------------------------------------------------------------------------------

  /**
   * Scenario 19: restore must persist a literal TABLE. The archive has no discriminator column, and
   * a stored NULL would still read back as TABLE, so only the raw column proves the stamp.
   */
  @Test
  public void restoreStampsTheTableDiscriminator() {
    String database = databaseId("restore_discriminator");
    http.createTable(database, "t_sd");
    Map<String, Object> column = readEntityTypeColumn(database, "t_sd");
    assertThat(isNull(column)).as("a created table stores a literal TABLE").isFalse();
    assertThat(column.get("stored_value")).isEqualTo("TABLE");

    expectStatus(http.softDeleteTable(database, "t_sd"), 204, "soft deleting the table");
    assertThat(countLiveRows(database, "t_sd"))
        .as("a soft deleted table leaves the live store")
        .isZero();

    List<JsonNode> softDeleted = http.querySoftDeleted(database, "t_sd");
    assertThat(softDeleted).hasSize(1);
    JsonNode row = softDeleted.get(0);
    // Deliberate: there is no column behind this field, so it must serialize as JSON null.
    assertThat(row.has("entityType") && row.get("entityType").isNull())
        .as("the soft deleted projection carries an explicit null entityType: %s", row)
        .isTrue();
    assertThat(row.path("deletedAtMs").isNumber())
        .as("deletedAtMs must be stamped: %s", row)
        .isTrue();

    Response response = http.restoreTable(database, "t_sd", row.get("deletedAtMs").asLong());
    expectStatus(response, 200, "PUT /hts/tables/restore");

    column = readEntityTypeColumn(database, "t_sd");
    assertThat(isNull(column))
        .as("restore must store TABLE, not rely on the NULL-to-TABLE read default")
        .isFalse();
    assertThat(column.get("stored_value"))
        .as("restore persists the literal TABLE")
        .isEqualTo("TABLE");

    // Secondary: equally true of a stored NULL.
    expectEntityType(response, "TABLE");
    Response restored = http.getEntity("tables", database, "t_sd");
    expectStatus(restored, 200, "GET /hts/tables on the restored row");
    expectEntityType(restored, "TABLE");
  }

  /** Scenario 20. Timestamps come from responses, never from a guessed clock. */
  @Test
  public void softDeleteRestoreAndPurgeLifecycle() {
    String database = databaseId("soft_delete");
    String location = text(http.createTable(database, "t1"), "metadataLocation");

    expectStatus(http.softDeleteTable(database, "t1"), 204, "DELETE /v1/hts/tables?isSoftDelete");
    expectStatus(
        http.getEntity("tables", database, "t1"), 404, "a soft delete leaves the live store");

    List<JsonNode> softDeleted = http.querySoftDeleted(database, "t1");
    assertThat(softDeleted).hasSize(1);
    JsonNode row = softDeleted.get(0);
    assertThat(text(row, "tableId")).isEqualTo("t1");
    assertThat(text(row, "databaseId")).isEqualTo(database);
    assertThat(text(row, "metadataLocation")).isEqualTo(location);
    assertThat(row.path("deletedAtMs").isNumber())
        .as("deletedAtMs must be stamped: %s", row)
        .isTrue();
    assertThat(row.path("purgeAfterMs").isNumber()).as("purgeAfterMs: %s", row).isTrue();
    long deletedAtMs = row.get("deletedAtMs").asLong();
    assertThat(row.get("purgeAfterMs").asLong())
        .as("purgeAfterMs is a TTL beyond deletedAtMs")
        .isGreaterThan(deletedAtMs);

    Response response = http.restoreTable(database, "t1", deletedAtMs);
    expectStatus(response, 200, "PUT /hts/tables/restore");
    expectEntityType(response, "TABLE");
    assertThat(text(response.entity(), "metadataLocation")).isEqualTo(location);
    expectStatus(http.getEntity("tables", database, "t1"), 200, "a restored table is live again");
    assertThat(http.querySoftDeleted(database, "t1")).as("restore removes the archive").isEmpty();

    expectStatus(http.restoreTable(database, "t1", deletedAtMs), 409, "restore over a live table");

    expectStatus(http.softDeleteTable(database, "t1"), 204, "second soft delete");
    assertThat(http.querySoftDeleted(database, "t1")).as("re-soft-delete records a row").hasSize(1);

    expectStatus(http.purgeTable(database, "t1"), 204, "DELETE /hts/tables/purge");
    assertThat(http.querySoftDeleted(database, "t1")).as("purge removes the archive").isEmpty();
    expectStatus(http.getEntity("entities", database, "t1"), 404, "a purge does not revive it");
    expectStatus(http.restoreTable(database, "t1", deletedAtMs), 404, "restore after purge");
  }

  /** Scenario 21: views are always hard deleted; there is no soft-deleted view store. */
  @Test
  public void viewDropProducesNoSoftDeletedRow() {
    String database = databaseId("view_no_soft_delete");
    http.createView(database, "v_sd");
    http.createTable(database, "sibling");

    Response response = http.softDeleteTable(database, "v_sd");
    expectStatus(response, 404, "soft delete through the table route on a view");
    expectMessage(
        response, "User table " + database + ".v_sd cannot be found", "soft delete on a view");
    expectStatus(http.getEntity("views", database, "v_sd"), 200, "a refused soft delete keeps it");
    assertThat(http.querySoftDeleted(database, "v_sd"))
        .as("no archive for a refused view")
        .isEmpty();

    expectStatus(http.deleteEntity("views", database, "v_sd"), 204, "dropping the view");
    expectStatus(http.getEntity("entities", database, "v_sd"), 404, "the dropped view is gone");
    assertThat(http.querySoftDeleted(database, "v_sd")).as("no archive at its own key").isEmpty();
    assertThat(http.querySoftDeleted(database, null)).as("no archive anywhere").isEmpty();

    // A table dropped the same way is archived, so the empty results above are real scoping.
    expectStatus(http.softDeleteTable(database, "sibling"), 204, "soft deleting the table");
    assertThat(tableIds(http.querySoftDeleted(database, null))).containsExactly("sibling");
  }

  // ---------------------------------------------------------------------------------------------
  // 22-23: stored discriminators no request can write
  // ---------------------------------------------------------------------------------------------

  /**
   * Scenario 22: a row predating the discriminator behaves as a TABLE everywhere. The delete pair
   * is separate from the read pair because the delete predicate carries its own NULL arm.
   */
  @Test
  public void legacyNullEntityTypeResolvesAsTable() {
    String database = databaseId("legacy_null");
    userTableRawSeeder.seedLegacyRow(rawRow(database, "t_legacy"));

    Map<String, Object> column = readEntityTypeColumn(database, "t_legacy");
    assertThat(isNull(column)).as("the planted row holds SQL NULL").isTrue();
    assertThat(column.get("stored_value")).isNull();

    Response response = http.getEntity("tables", database, "t_legacy");
    expectStatus(response, 200, "GET /hts/tables on a legacy NULL row");
    expectEntityType(response, "TABLE");
    expectStatus(http.getEntity("views", database, "t_legacy"), 404, "GET /hts/views on it");

    List<JsonNode> tables = http.queryEntities("tables", database);
    assertThat(tableIds(tables)).containsExactly("t_legacy");
    assertThat(text(tables.get(0), "entityType")).isEqualTo("TABLE");
    assertThat(http.queryEntities("views", database)).as("views exclude a NULL row").isEmpty();

    expectStatus(http.deleteEntity("views", database, "t_legacy"), 404, "DELETE /hts/views on it");
    assertThat(countLiveRows(database, "t_legacy")).as("a view delete keeps it").isEqualTo(1);

    expectStatus(http.deleteEntity("tables", database, "t_legacy"), 204, "DELETE /hts/tables");
    assertThat(countLiveRows(database, "t_legacy")).as("a table delete removes it").isZero();
  }

  /**
   * Scenario 23: corrupt discriminators fail in one of two ways, decided by the column collation. A
   * value the collation equates with TABLE passes the typed predicate and fails conversion (500);
   * anything else is invisible to the typed routes (404) yet still breaks the neutral read (500).
   *
   * <p>Rows are planted and compared through a separate unpooled session pinned to {@code
   * utf8mb4_0900_ai_ci}; that pin describes this inspection session only. The HTTP requests run
   * through the service pool unmodified. This characterizes known behaviour; it does not fix it.
   */
  @Test
  public void corruptEntityTypeValuesSplitIntoTwoFailureModes() throws SQLException {
    String database = databaseId("corrupt");
    Map<String, Integer> cases = new LinkedHashMap<>();
    cases.put("TABLE", 200);
    cases.put("T\u00c1BLE", 500);
    cases.put("TABLE ", 404);
    cases.put(" TABLE", 404);
    cases.put("", 404);
    cases.put("   ", 404);
    Map<String, String> tables = new LinkedHashMap<>();
    int index = 0;
    for (String stored : cases.keySet()) {
      tables.put(stored, "t_case" + index++);
    }

    try (Connection connection = openInspectionConnection()) {
      JdbcOperations inspection =
          new JdbcTemplate(new SingleConnectionDataSource(connection, true));
      inspection.execute("SET NAMES utf8mb4 COLLATE " + COLUMN_COLLATION);
      assertThat(inspection.queryForObject("SELECT DATABASE()", String.class)).isEqualTo(DATABASE);
      assertThat(inspection.queryForObject("SELECT @@collation_connection", String.class))
          .as("the inspection session compares under NO PAD, not PAD SPACE")
          .isEqualTo(COLUMN_COLLATION);
      assertThat(
              inspection.queryForObject(
                  "SELECT collation_name FROM information_schema.columns"
                      + " WHERE table_schema = DATABASE() AND table_name = 'user_table_row'"
                      + " AND column_name = 'entity_type'",
                  String.class))
          .as("the expectations below assume an accent-insensitive NO PAD column")
          .isEqualTo(COLUMN_COLLATION);
      try {
        for (Map.Entry<String, String> planted : tables.entrySet()) {
          UserTableRawSeeder.seedStoredEntityType(
              inspection, rawRow(database, planted.getValue()), planted.getKey());
        }

        for (Map.Entry<String, Integer> corruptCase : cases.entrySet()) {
          String stored = corruptCase.getKey();
          int expectedStatus = corruptCase.getValue();
          String table = tables.get(stored);
          String what = "entity_type '" + stored + "'";

          assertThat(
                  inspection.queryForObject(
                      "SELECT entity_type = 'TABLE' FROM user_table_row"
                          + " WHERE database_id = ? AND table_id = ?",
                      Integer.class,
                      database,
                      table))
              .as("the database's own comparison for %s", what)
              .isEqualTo(expectedStatus == 404 ? 0 : 1);

          Response typed = http.getEntity("tables", database, table);
          expectStatus(typed, expectedStatus, "GET /hts/tables on " + what);
          if (expectedStatus == 500) {
            expectCorruptValueDiagnostic(typed, stored, "GET /hts/tables on " + what);
          }
          if (!"TABLE".equals(stored)) {
            Response neutral = http.getEntity("entities", database, table);
            expectStatus(neutral, 500, "GET /hts/entities on " + what);
            expectCorruptValueDiagnostic(neutral, stored, "GET /hts/entities on " + what);
          }
        }

        // KNOWN BAD, pinned so a fix fails here loudly: NO PAD hides a trailing space from the
        // typed predicate while the neutral read still hydrates it.
        Response typed = http.getEntity("tables", database, tables.get("TABLE "));
        Response neutral = http.getEntity("entities", database, tables.get("TABLE "));
        assertThat(Arrays.asList(existence(typed), existence(neutral)))
            .as("trailing-space disagreement. %s || %s", typed.describe(), neutral.describe())
            .containsExactly(Existence.ABSENT, Existence.ERROR);

        // An accented value reaches the converter on both routes, so they agree on error.
        typed = http.getEntity("tables", database, tables.get("T\u00c1BLE"));
        neutral = http.getEntity("entities", database, tables.get("T\u00c1BLE"));
        assertThat(Arrays.asList(existence(typed), existence(neutral)))
            .as("accented agreement. %s || %s", typed.describe(), neutral.describe())
            .containsExactly(Existence.ERROR, Existence.ERROR);

        // One unconvertible row fails the whole listing rather than being silently omitted.
        Response listing = http.get("/hts/tables/query", params("databaseId", database));
        expectStatus(listing, 500, "GET /hts/tables/query over one unconvertible row");
        assertThat(listing.message()).as(listing.describe()).contains("user_table_row.entity_type");
      } finally {
        inspection.update("DELETE FROM user_table_row WHERE database_id = ?", database);
        inspection.update(
            "DELETE FROM soft_deleted_user_table_row WHERE database_id = ?", database);
      }
    }
  }

  // ---------------------------------------------------------------------------------------------
  // 24: request validation. HTS is unauthenticated, so a bad request is 400, not 401.
  // ---------------------------------------------------------------------------------------------

  /** Scenario 24. */
  @Test
  public void missingRequiredParametersAreBadRequest() {
    Map<String, Response> requests = new LinkedHashMap<>();
    requests.put("GET /hts/tables", http.get("/hts/tables", params()));
    requests.put("GET /hts/views", http.get("/hts/views", params()));
    requests.put("GET /hts/entities", http.get("/hts/entities", params()));
    requests.put(
        "GET /hts/tables/querySoftDeleted", http.get("/hts/tables/querySoftDeleted", params()));
    requests.put(
        "PATCH /hts/tables/rename",
        http.exchange(HttpMethod.PATCH, "/hts/tables/rename", params(), null));
    requests.put(
        "DELETE /v1/hts/tables without isSoftDelete",
        http.exchange(
            HttpMethod.DELETE, "/v1/hts/tables", params("databaseId", "d", "tableId", "t"), null));
    assertThat(requests).hasSize(6);
    requests.forEach((what, response) -> expectStatus(response, 400, what));
  }
}

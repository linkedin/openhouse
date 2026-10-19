package com.linkedin.openhouse.tables.e2e.h2;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.MAX_VIEW_SCHEMA_BYTES;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.MAX_VIEW_SQL_BYTES;
import static com.linkedin.openhouse.internal.catalog.mapper.HouseTableSerdeUtils.getCanonicalFieldName;
import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.linkedin.openhouse.cluster.configs.ClusterProperties;
import com.linkedin.openhouse.common.security.DummyTokenInterceptor;
import com.linkedin.openhouse.tables.exception.ViewExceptionHandler;
import com.linkedin.openhouse.tables.mock.properties.AuthorizationPropertiesInitializer;
import com.linkedin.openhouse.tables.model.ViewModelConstants;
import com.linkedin.openhouse.tables.toggle.model.TableToggleStatus;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.iceberg.view.SQLViewRepresentation;
import org.apache.iceberg.view.ViewMetadata;
import org.apache.iceberg.view.ViewMetadataParser;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Import;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.web.util.UriComponentsBuilder;

/**
 * Real TCP requests to a random-port Tables Service, then Docker HTS and Docker MySQL. No view,
 * catalog, admission, toggle, storage, or authorization component is mocked. Local dummy JWTs
 * exercise the supported test authentication boundary, not production token validation or OPA.
 */
@SpringBootTest(
    classes = SpringH2Application.class,
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = "cluster.tables.views.supported-dialects=spark,trino")
@ContextConfiguration(
    initializers = {TableE2eContextInitializer.class, AuthorizationPropertiesInitializer.class})
@Import(ViewExceptionHandler.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
@EnabledIfSystemProperty(named = "tableE2eBackend", matches = "docker")
public class ViewsDockerE2eTest {

  private static final String CREATOR = "views-e2e-user";
  private static final String VIEW = "test_view";
  private static final String SEED_TABLE = "database_seed";
  private static final String SQL = "SELECT id, name FROM source_table";

  @Autowired private TestRestTemplate tables;
  @Autowired private ObjectMapper mapper;
  @Autowired private TableE2eFixtures fixtures;
  @Autowired private ClusterProperties clusterProperties;

  private final TestRestTemplate hts =
      new TestRestTemplate(
          new RestTemplateBuilder()
              .setConnectTimeout(Duration.ofSeconds(10))
              .setReadTimeout(Duration.ofSeconds(60)));
  private final Set<String> databases = new LinkedHashSet<>();
  private final Set<String> views = new LinkedHashSet<>();
  private final Set<String> tableNames = new LinkedHashSet<>();
  private final List<TableToggleStatus> toggles = new ArrayList<>();
  private String database;
  private HttpHeaders headers;

  @BeforeEach
  void setUp() throws Exception {
    assertTrue(fixtures.usesDocker(), "This suite must never run against an H2 substitute");
    fixtures.assertBackendWiring();
    headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setBearerAuth(new DummyTokenInterceptor.DummySecurityJWT(CREATOR).buildNoopJWT());
    database = "views_e2e_" + UUID.randomUUID().toString().replace("-", "");
    seedDatabase(database);
    enableViews(database);
  }

  @AfterEach
  void cleanUp() throws Exception {
    // Cleanup uses typed HTS DELETE even if a deliberately rejected Tables request failed.
    // It never converts a failed lifecycle assertion into success.
    for (String db : databases) {
      Set<String> names = new LinkedHashSet<>(views);
      names.addAll(tableNames);
      for (String name : names) {
        ResponseEntity<String> occupant = htsRead("entities", db, name);
        if (occupant.getStatusCodeValue() == 200) {
          String type =
              mapper.readTree(occupant.getBody()).path("entity").path("entityType").asText();
          assertTrue("VIEW".equals(type) || "TABLE".equals(type), type);
          deleteFixture("VIEW".equals(type) ? "views" : "tables", db, name);
        } else {
          assertEquals(404, occupant.getStatusCodeValue(), occupant.getBody());
        }
        assertFalse(fixtures.persistedEntity(db, name).isPresent());
      }
    }
    for (TableToggleStatus toggle : toggles) {
      fixtures.deleteToggle(toggle);
    }
  }

  @Test
  void createGetReplaceListDeletePersistsTheRealViewPointerAndMetadata() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    assertEquals(0L, ((Number) before.get("version")).longValue());
    ViewMetadata first = metadata(created);
    assertEquals(1, first.currentVersionId());
    assertEquals(SQL, sql(first));
    assertEquals("spark", first.currentVersion().summary().get("sourceDialect"));
    assertEquals("create", first.currentVersion().summary().get("operation"));
    assertEquals("openhouse", first.currentVersion().defaultCatalog());
    assertEquals(
        Collections.singletonList(database),
        Arrays.asList(first.currentVersion().defaultNamespace().levels()));
    assertEquals("initial", first.properties().get("description"));
    assertEquals(CREATOR, first.properties().get(getCanonicalFieldName("tableCreator")));
    assertEquals(
        created.path("creationTime").asLong(),
        Long.parseLong(first.properties().get(getCanonicalFieldName("creationTime"))));
    assertEquals(first.uuid(), first.properties().get(getCanonicalFieldName("tableUUID")));
    assertTrue(first.location().contains(first.uuid()));
    UUID.fromString(first.uuid());
    assertTrue(created.path("creationTime").asLong() > 0);
    assertEquals(CREATOR, created.path("viewCreator").asText());
    assertPointerShape(created);

    JsonNode fetched = request(HttpMethod.GET, viewPath(database, VIEW), null, 200);
    assertPointerShape(fetched);
    assertEquals(created.path("metadataLocation"), fetched.path("metadataLocation"));
    assertEquals(created.path("creationTime"), fetched.path("creationTime"));

    ObjectNode changed = payload(database, VIEW);
    changed.put("baseMetadataLocation", pointer(created));
    ((ObjectNode) changed.withArray("representations").get(0))
        .put("sql", "SELECT name FROM source_table");
    JsonNode updated = request(HttpMethod.PUT, viewPath(database, VIEW), changed, 200);
    Map<String, Object> after = assertViewPersisted(database, VIEW, updated);
    ViewMetadata second = metadata(updated);
    assertNotEquals(pointer(created), pointer(updated));
    assertEquals(first.uuid(), second.uuid());
    assertEquals(first.location(), second.location());
    assertEquals(2, second.currentVersionId());
    assertEquals("replace", second.currentVersion().summary().get("operation"));
    assertEquals(2, second.history().size());
    assertEquals(before.get("creation_time"), after.get("creation_time"));
    assertEquals(pointer(created), second.properties().get(getCanonicalFieldName("tableVersion")));
    assertEquals(
        ((Number) before.get("version")).longValue() + 1,
        ((Number) after.get("version")).longValue());
    assertTrue(
        updated.path("lastModifiedTime").asLong() > created.path("lastModifiedTime").asLong());
    assertEquals(
        pointer(updated),
        request(HttpMethod.GET, viewPath(database, VIEW), null, 200)
            .path("metadataLocation")
            .asText());
    long filesBeforeStale = metadataFileCount();
    request(HttpMethod.PUT, viewPath(database, VIEW), changed, 409);
    assertEquals(after, assertViewPersisted(database, VIEW, updated));
    assertEquals(filesBeforeStale, metadataFileCount());

    JsonNode listed = request(HttpMethod.GET, collection(database) + "?size=1", null, 200);
    assertEquals(1, listed.path("results").size());
    assertEquals(VIEW, listed.path("results").get(0).path("viewId").asText());
    assertIdentifierShape(listed.path("results").get(0));
    assertFalse(listed.hasNonNull("nextPageToken"));

    request(HttpMethod.DELETE, viewPath(database, VIEW), null, 204);
    assertAbsent(database, VIEW);
    request(HttpMethod.GET, viewPath(database, VIEW), null, 404);
    request(HttpMethod.DELETE, viewPath(database, VIEW), null, 404);
    assertEquals(
        0, request(HttpMethod.GET, collection(database), null, 200).path("results").size());
  }

  @Test
  void creatorAndModificationMetadataSurviveRealHtsReadsAndReplacement() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    JsonNode fetched = request(HttpMethod.GET, viewPath(database, VIEW), null, 200);
    ObjectNode update = payload(database, VIEW);
    update.put("baseMetadataLocation", pointer(created));
    update.with("viewProperties").put("description", "changed");
    headers.setBearerAuth(
        new DummyTokenInterceptor.DummySecurityJWT("views-e2e-editor").buildNoopJWT());
    JsonNode updated = request(HttpMethod.PUT, viewPath(database, VIEW), update, 200);
    JsonNode readBack = request(HttpMethod.GET, viewPath(database, VIEW), null, 200);
    assertAll(
        () -> assertEquals(created.path("viewCreator"), fetched.path("viewCreator")),
        () -> assertEquals(created.path("lastModifiedTime"), fetched.path("lastModifiedTime")),
        () -> assertEquals(CREATOR, updated.path("viewCreator").asText()),
        () -> assertEquals(CREATOR, readBack.path("viewCreator").asText()),
        () -> assertEquals(updated.path("lastModifiedTime"), readBack.path("lastModifiedTime")),
        () -> assertEquals(created.path("creationTime"), readBack.path("creationTime")));
    assertViewPersisted(database, VIEW, readBack);
  }

  @Test
  void putCreatesOnlyWithInitialVersionAndDeletedNameAllocatesANewIdentity() throws Exception {
    views.add(VIEW);
    ObjectNode stale = payload(database, VIEW).put("baseMetadataLocation", "file:/stale-marker");
    long files = metadataFileCount();
    request(HttpMethod.PUT, viewPath(database, VIEW), stale, 409);
    assertAbsent(database, VIEW);
    assertEquals(files, metadataFileCount());
    JsonNode created =
        request(
            HttpMethod.PUT,
            viewPath(database, VIEW),
            payload(database, VIEW).put("baseMetadataLocation", INITIAL_TABLE_VERSION),
            201);
    assertViewPersisted(database, VIEW, created);
    String uuid = metadata(created).uuid();
    request(HttpMethod.DELETE, viewPath(database, VIEW), null, 204);
    assertAbsent(database, VIEW);
    JsonNode recreated = create(VIEW, payload(database, VIEW), 201);
    assertNotEquals(uuid, metadata(recreated).uuid());
    assertNotEquals(pointer(created), pointer(recreated));
    assertViewPersisted(database, VIEW, recreated);
  }

  @Test
  void unchangedPutDoesNotTouchMysqlOrAllocateMetadata() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    long files = metadataFileCount();
    ObjectNode unchanged = payload(database, VIEW).put("baseMetadataLocation", pointer(created));
    JsonNode first = request(HttpMethod.PUT, viewPath(database, VIEW), unchanged, 200);
    JsonNode second = request(HttpMethod.PUT, viewPath(database, VIEW), unchanged, 200);
    assertEquals(pointer(created), pointer(first));
    assertEquals(pointer(first), pointer(second));
    assertEquals(before, assertViewPersisted(database, VIEW, second));
    assertEquals(files, metadataFileCount());
    assertEquals(created.path("lastModifiedTime"), second.path("lastModifiedTime"));
    assertEquals(1, metadata(second).history().size());
  }

  @Test
  void duplicatePostAndStalePutLeaveTheCompletePhysicalRowUnchanged() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    long files = metadataFileCount();
    create(VIEW, payload(database, VIEW), 409);
    ObjectNode stale = payload(database, VIEW).put("baseMetadataLocation", INITIAL_TABLE_VERSION);
    stale.with("viewProperties").put("description", "must_not_publish");
    request(HttpMethod.PUT, viewPath(database, VIEW), stale, 409);
    assertEquals(before, assertViewPersisted(database, VIEW, created));
    assertEquals(files, metadataFileCount());
    assertEquals("initial", metadata(created).properties().get("description"));
  }

  @Test
  void propertyMergeAndSchemaEvolutionPreserveUuidAndEarlierMetadata() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    ViewMetadata first = metadata(created);
    ObjectNode update = payload(database, VIEW).put("baseMetadataLocation", pointer(created));
    update.set("viewProperties", mapper.createObjectNode().put("new_property", "new_value"));
    update.put(
        "schema",
        "{\"type\":\"struct\",\"fields\":["
            + "{\"id\":1,\"required\":true,\"name\":\"id\",\"type\":\"string\"},"
            + "{\"id\":2,\"required\":true,\"name\":\"name\",\"type\":\"string\"},"
            + "{\"id\":3,\"required\":false,\"name\":\"extra\",\"type\":\"long\"}]}");
    JsonNode updated = request(HttpMethod.PUT, viewPath(database, VIEW), update, 200);
    assertViewPersisted(database, VIEW, updated);
    ViewMetadata second = metadata(updated);
    assertEquals(first.uuid(), second.uuid());
    assertEquals(3, second.schema().columns().size());
    assertEquals(2, second.schemas().size());
    assertEquals("initial", second.properties().get("description"));
    assertEquals("new_value", second.properties().get("new_property"));
    assertEquals(2, metadata(created).schema().columns().size());
    assertFalse(metadata(created).properties().containsKey("new_property"));
  }

  @Test
  void multipleDialectsArePersistedAndDroppingARequiredDialectIsRejected() throws Exception {
    ObjectNode body = payload(database, VIEW);
    body.withArray("representations")
        .add(
            mapper
                .createObjectNode()
                .put("type", "sql")
                .put("dialect", "trino")
                .put("sql", "SELECT id FROM source_table"));
    JsonNode created = create(VIEW, body, 201);
    assertEquals(2, metadata(created).currentVersion().representations().size());
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    ObjectNode dropping = payload(database, VIEW).put("baseMetadataLocation", pointer(created));
    long files = metadataFileCount();
    JsonNode error = request(HttpMethod.PUT, viewPath(database, VIEW), dropping, 500);
    assertFalse(error.hasNonNull("cause"));
    assertFalse(error.hasNonNull("stacktrace"));
    assertFalse(error.toString().contains(SQL));
    assertEquals(before, assertViewPersisted(database, VIEW, created));
    assertEquals(files, metadataFileCount());
  }

  @Test
  void mysqlCaseInsensitiveIdentityAndDatabaseScopedFeatureGateAgree() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    String upperDb = database.toUpperCase(Locale.ROOT);
    String upperView = VIEW.toUpperCase(Locale.ROOT);
    JsonNode fetched = request(HttpMethod.GET, viewPath(upperDb, upperView), null, 200);
    assertEquals(pointer(created), pointer(fetched));
    request(HttpMethod.POST, collection(upperDb), payload(upperDb, upperView), 409);
    assertEquals(before, assertViewPersisted(database, VIEW, created));
    request(HttpMethod.DELETE, viewPath(upperDb, upperView), null, 204);
    assertAbsent(database, VIEW);
  }

  @Test
  void tablesAndViewsAreIsolatedInBothServicesAndShareTheNameCollisionBoundary() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> viewBefore = assertViewPersisted(database, VIEW, created);
    request(HttpMethod.GET, tablesPath(database) + "/" + VIEW, null, 404);
    request(HttpMethod.POST, tablesPath(database), tablePayload(database, VIEW), 409);
    request(HttpMethod.PUT, tablesPath(database) + "/" + VIEW, tablePayload(database, VIEW), 409);
    assertEquals(viewBefore, assertViewPersisted(database, VIEW, created));
    Map<String, Object> tableBefore = fixtures.persistedEntity(database, SEED_TABLE).orElseThrow();
    create(SEED_TABLE, payload(database, SEED_TABLE), 409);
    request(
        HttpMethod.PUT,
        viewPath(database, SEED_TABLE),
        payload(database, SEED_TABLE).put("baseMetadataLocation", INITIAL_TABLE_VERSION),
        409);
    request(HttpMethod.GET, viewPath(database, SEED_TABLE), null, 404);
    request(HttpMethod.DELETE, viewPath(database, SEED_TABLE), null, 404);
    assertEquals(tableBefore, fixtures.persistedEntity(database, SEED_TABLE).orElseThrow());
    assertEquals("TABLE", tableBefore.get("entity_type"));
    JsonNode listed = request(HttpMethod.POST, tablesPath(database) + "/search", null, 200);
    assertEquals(1, listed.path("results").size());
    assertEquals(SEED_TABLE, listed.path("results").get(0).path("tableId").asText());
    request(HttpMethod.DELETE, viewPath(database, VIEW), null, 204);
    assertAbsent(database, VIEW);
    tableNames.add(VIEW);
    JsonNode table =
        request(HttpMethod.POST, tablesPath(database), tablePayload(database, VIEW), 201);
    assertEquals(
        "TABLE", fixtures.persistedEntity(database, VIEW).orElseThrow().get("entity_type"));
    assertNotNull(table.path("tableLocation").textValue());
    assertEquals(404, htsRead("views", database, VIEW).getStatusCodeValue());
  }

  @Test
  void sameViewNameInDifferentDatabasesHasIndependentPointersAndNamespaces() throws Exception {
    String other = database + "_other";
    seedDatabase(other);
    enableViews(other);
    JsonNode first = create(VIEW, payload(database, VIEW), 201);
    JsonNode second = request(HttpMethod.POST, collection(other), payload(other, VIEW), 201);
    assertViewPersisted(other, VIEW, second);
    assertNotEquals(metadata(first).uuid(), metadata(second).uuid());
    assertNotEquals(pointer(first), pointer(second));
    request(HttpMethod.DELETE, viewPath(database, VIEW), null, 204);
    assertAbsent(database, VIEW);
    assertEquals(
        pointer(second),
        request(HttpMethod.GET, viewPath(other, VIEW), null, 200)
            .path("metadataLocation")
            .asText());
    assertEquals(other, metadata(second).currentVersion().defaultNamespace().level(0));
  }

  @Test
  void paginationCrossesHtsSourcePagesWithoutDuplicatesAndBindsTokensToDatabase() throws Exception {
    List<String> expected = new ArrayList<>();
    for (int index = 0; index < 53; index++) {
      String view = String.format(Locale.ROOT, "paged_%03d", index);
      expected.add(view);
      assertViewPersisted(database, view, create(view, payload(database, view), 201));
    }
    JsonNode first =
        request(HttpMethod.GET, collection(database) + "?size=7&sortBy=VIEWID", null, 200);
    String firstToken = first.path("nextPageToken").asText();
    assertFalse(firstToken.isEmpty());
    List<String> actual = new ArrayList<>();
    JsonNode page = first;
    Set<String> tokens = new HashSet<>();
    int pages = 0;
    while (true) {
      for (JsonNode result : page.path("results")) {
        assertIdentifierShape(result);
        assertEquals(database, result.path("databaseId").asText());
        actual.add(result.path("viewId").asText());
      }
      pages++;
      assertTrue(pages <= 10, "Continuation must terminate");
      if (!page.hasNonNull("nextPageToken")) {
        break;
      }
      String token = page.path("nextPageToken").asText();
      assertTrue(tokens.add(token), "Continuation must advance");
      page = request(HttpMethod.GET, continuation(database, token, 11), null, 200);
    }
    assertEquals(expected, actual);
    assertEquals(expected.size(), new HashSet<>(actual).size());
    String other = database + "_other";
    seedDatabase(other);
    enableViews(other);
    request(HttpMethod.GET, continuation(other, firstToken, 11), null, 400);
    assertEquals(
        expected.size(),
        request(HttpMethod.GET, collection(database) + "?size=2147483647", null, 200)
            .path("results")
            .size());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "size=0",
        "size=-1",
        "size=abc",
        "sortBy=creationTime",
        "sortBy=viewId:desc",
        "sortBy=viewId,databaseId",
        "page=0",
        "pageToken=broken",
        "pageToken="
      })
  void invalidListingParametersDoNotMutatePersistence(String query) throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    request(HttpMethod.GET, collection(database) + "?" + query, null, 400);
    assertEquals(before, assertViewPersisted(database, VIEW, created));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "schema",
        "representations",
        "sourceDialect",
        "viewId",
        "databaseId",
        "reservedProperty",
        "policies",
        "duplicateDialect",
        "unsupportedDialect",
        "emptyNamespace",
        "badSchema",
        "postBase",
        "badSqlType",
        "emptySql"
      })
  void rejectedCreateDefinitionsAllocateNeitherRowsNorMetadata(String invalid) throws Exception {
    views.add(VIEW);
    ObjectNode body = payload(database, VIEW);
    switch (invalid) {
      case "reservedProperty":
        body.with("viewProperties").put("openhouse.tableUUID", "caller-owned");
        break;
      case "policies":
        body.with("viewProperties").put("policies", "{}");
        break;
      case "duplicateDialect":
        body.withArray("representations").add(body.withArray("representations").get(0).deepCopy());
        break;
      case "unsupportedDialect":
        ((ObjectNode) body.withArray("representations").get(0)).put("dialect", "unsupported");
        break;
      case "emptyNamespace":
        body.putArray("defaultNamespace");
        break;
      case "badSchema":
        body.put("schema", "private-schema-marker");
        break;
      case "postBase":
        body.put("baseMetadataLocation", "private-pointer-marker");
        break;
      case "badSqlType":
        ((ObjectNode) body.withArray("representations").get(0)).put("type", "other");
        break;
      case "emptySql":
        ((ObjectNode) body.withArray("representations").get(0)).put("sql", "");
        break;
      default:
        body.remove(invalid);
    }
    long files = metadataFileCount();
    JsonNode error = request(HttpMethod.POST, collection(database), body, 400);
    assertFalse(error.hasNonNull("cause"));
    assertFalse(error.hasNonNull("stacktrace"));
    assertFalse(error.toString().contains("private-schema-marker"));
    assertFalse(error.toString().contains("private-pointer-marker"));
    assertAbsent(database, VIEW);
    assertEquals(files, metadataFileCount());
  }

  @Test
  void invalidPutAndUrlBodyMismatchesLeaveExistingPointerUntouched() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    long files = metadataFileCount();
    request(HttpMethod.PUT, viewPath(database, VIEW), payload(database, VIEW), 400);
    request(
        HttpMethod.PUT,
        viewPath(database, VIEW),
        payload(database, VIEW).put("baseMetadataLocation", " "),
        400);
    request(
        HttpMethod.PUT,
        viewPath(database, VIEW),
        payload(database, "other_view").put("baseMetadataLocation", pointer(created)),
        400);
    request(HttpMethod.POST, collection(database), payload(database + "_other", VIEW), 400);
    request(HttpMethod.POST, collection(database), "{not-json", 400);
    assertEquals(before, assertViewPersisted(database, VIEW, created));
    assertEquals(files, metadataFileCount());
  }

  @Test
  void missingDatabaseAndDefaultNamespaceAreRejectedBeforeAllocating() throws Exception {
    views.add(VIEW);
    String absent = database + "_missing";
    ObjectNode body = payload(absent, VIEW);
    enableViews(absent);
    request(HttpMethod.POST, collection(absent), body, 404);
    assertAbsent(absent, VIEW);
    body = payload(database, VIEW);
    body.putArray("defaultNamespace").add(absent);
    long files = metadataFileCount();
    request(HttpMethod.POST, collection(database), body, 404);
    assertAbsent(database, VIEW);
    assertEquals(files, metadataFileCount());
  }

  @Test
  void enablingOneDatabaseDoesNotEnableViewsInAnotherDatabase() throws Exception {
    String disabledDatabase = database + "_disabled";
    seedDatabase(disabledDatabase);
    JsonNode enabledView = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> enabledBefore = assertViewPersisted(database, VIEW, enabledView);
    long filesBefore = metadataFileCount();

    ObjectNode disabledBody = payload(disabledDatabase, VIEW);
    request(HttpMethod.POST, collection(disabledDatabase), disabledBody, 404);
    request(
        HttpMethod.PUT,
        viewPath(disabledDatabase, VIEW),
        disabledBody.deepCopy().put("baseMetadataLocation", INITIAL_TABLE_VERSION),
        404);
    request(HttpMethod.GET, collection(disabledDatabase), null, 404);
    request(HttpMethod.GET, viewPath(disabledDatabase, VIEW), null, 404);
    request(HttpMethod.DELETE, viewPath(disabledDatabase, VIEW), null, 404);
    assertAbsent(disabledDatabase, VIEW);
    assertEquals(filesBefore, metadataFileCount());
    assertEquals(
        enabledBefore,
        assertViewPersisted(
            database, VIEW, request(HttpMethod.GET, viewPath(database, VIEW), null, 200)));
    JsonNode enabledList = request(HttpMethod.GET, collection(database), null, 200);
    assertEquals(1, enabledList.path("results").size());
    assertEquals(VIEW, enabledList.path("results").get(0).path("viewId").asText());

    enableViews(disabledDatabase);
    JsonNode newlyEnabled =
        request(HttpMethod.POST, collection(disabledDatabase), disabledBody, 201);
    assertViewPersisted(disabledDatabase, VIEW, newlyEnabled);
    assertNotEquals(pointer(enabledView), pointer(newlyEnabled));
    assertEquals(enabledBefore, assertViewPersisted(database, VIEW, enabledView));
  }

  @Test
  void databaseScopedFeatureGateUsesRealMysqlRulesAndCannotBeEnabledByViewProperties()
      throws Exception {
    TableToggleStatus rule = toggles.remove(0);
    fixtures.deleteToggle(rule);
    views.add(VIEW);
    ObjectNode body = payload(database, VIEW);
    body.with("viewProperties").put("views.enabled", "true");
    long files = metadataFileCount();
    request(HttpMethod.POST, collection(database), body, 404);
    request(
        HttpMethod.PUT,
        viewPath(database, VIEW),
        body.deepCopy().put("baseMetadataLocation", INITIAL_TABLE_VERSION),
        404);
    request(HttpMethod.GET, collection(database), null, 404);
    request(HttpMethod.GET, viewPath(database, VIEW), null, 404);
    request(HttpMethod.DELETE, viewPath(database, VIEW), null, 404);
    assertAbsent(database, VIEW);
    assertEquals(files, metadataFileCount());
    enableViews(database);
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    rule = toggles.remove(0);
    fixtures.deleteToggle(rule);
    request(HttpMethod.DELETE, viewPath(database, VIEW), null, 404);
    assertEquals(before, assertViewPersisted(database, VIEW, created));
    enableViews(database);
    request(HttpMethod.DELETE, viewPath(database, VIEW), null, 204);
    assertAbsent(database, VIEW);
  }

  @Test
  void missingBearerAuthenticationIsRejectedForEveryRouteWithoutChangingMysql() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    HttpHeaders anonymous = new HttpHeaders();
    anonymous.setContentType(MediaType.APPLICATION_JSON);
    for (HttpMethod method : Arrays.asList(HttpMethod.GET, HttpMethod.PUT, HttpMethod.DELETE)) {
      ResponseEntity<String> response =
          tables.exchange(
              viewPath(database, VIEW),
              method,
              new HttpEntity<>(
                  payload(database, VIEW).put("baseMetadataLocation", pointer(created)).toString(),
                  anonymous),
              String.class);
      assertEquals(401, response.getStatusCodeValue(), response.getBody());
    }
    for (HttpMethod method : Arrays.asList(HttpMethod.GET, HttpMethod.POST)) {
      ResponseEntity<String> response =
          tables.exchange(
              collection(database),
              method,
              new HttpEntity<>(payload(database, VIEW).toString(), anonymous),
              String.class);
      assertEquals(401, response.getStatusCodeValue(), response.getBody());
    }
    assertEquals(before, assertViewPersisted(database, VIEW, created));
  }

  @ParameterizedTest
  @ValueSource(ints = {1, 2, 3})
  void concurrentReplacesPublishExactlyOneWinnerFromTheSameBase(int round) throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
    try {
      for (int index = 0; index < 2; index++) {
        ObjectNode body = payload(database, VIEW).put("baseMetadataLocation", pointer(created));
        body.with("viewProperties").put("winner", "writer_" + round + "_" + index);
        futures.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  assertTrue(start.await(30, TimeUnit.SECONDS));
                  return tables.exchange(
                      viewPath(database, VIEW),
                      HttpMethod.PUT,
                      new HttpEntity<>(body.toString(), headers),
                      String.class);
                }));
      }
      assertTrue(ready.await(30, TimeUnit.SECONDS));
      start.countDown();
      ResponseEntity<String> left = futures.get(0).get(90, TimeUnit.SECONDS);
      ResponseEntity<String> right = futures.get(1).get(90, TimeUnit.SECONDS);
      List<Integer> statuses =
          new ArrayList<>(Arrays.asList(left.getStatusCodeValue(), right.getStatusCodeValue()));
      Collections.sort(statuses);
      assertEquals(Arrays.asList(200, 409), statuses, left.getBody() + "\n" + right.getBody());
      JsonNode winner =
          mapper.readTree(left.getStatusCodeValue() == 200 ? left.getBody() : right.getBody());
      Map<String, Object> after = assertViewPersisted(database, VIEW, winner);
      assertEquals(
          ((Number) before.get("version")).longValue() + 1,
          ((Number) after.get("version")).longValue());
      assertEquals(
          pointer(created),
          metadata(winner).properties().get(getCanonicalFieldName("tableVersion")));
      assertEquals(
          "writer_" + round + "_" + (left.getStatusCodeValue() == 200 ? 0 : 1),
          metadata(winner).properties().get("winner"));
      assertEquals(metadata(created).uuid(), metadata(winner).uuid());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  void namespaceElementsReferenceExistingDatabasesAndArePreservedInMetadata() throws Exception {
    String other = database + "_other";
    seedDatabase(other);
    ObjectNode body = payload(database, VIEW);
    body.withArray("defaultNamespace").add(other.toUpperCase(Locale.ROOT));
    JsonNode created = create(VIEW, body, 201);
    assertViewPersisted(database, VIEW, created);
    assertEquals(
        Arrays.asList(database, other.toUpperCase(Locale.ROOT)),
        Arrays.asList(metadata(created).currentVersion().defaultNamespace().levels()));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "sql_exact",
        "sql_over",
        "schema_exact",
        "schema_over",
        "sql_utf8_exact",
        "sql_utf8_over"
      })
  void sqlAndSchemaByteLimitsAreInclusiveAndRejectOversizeBeforeAllocation(String limit)
      throws Exception {
    ObjectNode body = payload(database, VIEW);
    int excess = limit.endsWith("over") ? 1 : 0;
    if (limit.startsWith("schema")) {
      String schema = body.path("schema").asText();
      body.put("schema", schema + " ".repeat(MAX_VIEW_SCHEMA_BYTES - schema.length() + excess));
    } else {
      String sql =
          limit.contains("utf8")
              ? "\u00e9".repeat(MAX_VIEW_SQL_BYTES / 2) + "x".repeat(excess)
              : "x".repeat(MAX_VIEW_SQL_BYTES + excess);
      assertEquals(MAX_VIEW_SQL_BYTES + excess, sql.getBytes(StandardCharsets.UTF_8).length);
      ((ObjectNode) body.withArray("representations").get(0)).put("sql", sql);
    }
    views.add(VIEW);
    long files = metadataFileCount();
    if (excess == 0) {
      JsonNode created = create(VIEW, body, 201);
      assertViewPersisted(database, VIEW, created);
      if (limit.startsWith("sql")) {
        assertEquals(
            MAX_VIEW_SQL_BYTES, sql(metadata(created)).getBytes(StandardCharsets.UTF_8).length);
      } else {
        assertEquals(2, metadata(created).schema().columns().size());
      }
    } else {
      request(HttpMethod.POST, collection(database), body, 400);
      assertAbsent(database, VIEW);
      assertEquals(files, metadataFileCount());
    }
  }

  @Test
  void identifierLengthBoundaryAndIllegalCharactersAreEnforcedBeforeWrites() throws Exception {
    String valid = "v".repeat(128);
    JsonNode created = create(valid, payload(database, valid), 201);
    assertViewPersisted(database, valid, created);
    long files = metadataFileCount();
    for (String invalid : Arrays.asList("v".repeat(129), "dash-name", "dot.name", "space name")) {
      request(HttpMethod.POST, collection(database), payload(database, invalid), 400);
      assertFalse(fixtures.persistedEntity(database, invalid).isPresent());
    }
    assertEquals(files, metadataFileCount());
  }

  @Test
  void droppingATableMakesItsNameReusableAsAViewWithoutReusingItsUuid() throws Exception {
    String table = "reused_name";
    tableNames.add(table);
    JsonNode createdTable =
        request(HttpMethod.POST, tablesPath(database), tablePayload(database, table), 201);
    String tableUuid =
        mapper
            .readTree(
                Files.readString(
                    Paths.get(URI.create(createdTable.path("tableLocation").asText()))))
            .path("table-uuid")
            .asText();
    UUID.fromString(tableUuid);
    request(HttpMethod.DELETE, tablesPath(database) + "/" + table + "?purge=true", null, 204);
    assertAbsent(database, table);
    JsonNode createdView = create(table, payload(database, table), 201);
    assertViewPersisted(database, table, createdView);
    assertNotEquals(tableUuid, metadata(createdView).uuid());
    assertNotEquals(createdTable.path("tableLocation").asText(), pointer(createdView));
  }

  @Test
  void concurrentCreatesHaveOneMysqlWinnerAndNeverOverwriteItsDefinition() throws Exception {
    views.add(VIEW);
    ExecutorService executor = Executors.newFixedThreadPool(2);
    CountDownLatch ready = new CountDownLatch(2);
    CountDownLatch start = new CountDownLatch(1);
    List<Future<ResponseEntity<String>>> futures = new ArrayList<>();
    try {
      for (int index = 0; index < 2; index++) {
        ObjectNode body = payload(database, VIEW);
        body.with("viewProperties").put("winner", "creator_" + index);
        futures.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  assertTrue(start.await(30, TimeUnit.SECONDS));
                  return tables.exchange(
                      collection(database),
                      HttpMethod.POST,
                      new HttpEntity<>(body.toString(), headers),
                      String.class);
                }));
      }
      assertTrue(ready.await(30, TimeUnit.SECONDS));
      start.countDown();
      ResponseEntity<String> left = futures.get(0).get(90, TimeUnit.SECONDS);
      ResponseEntity<String> right = futures.get(1).get(90, TimeUnit.SECONDS);
      List<Integer> statuses =
          new ArrayList<>(Arrays.asList(left.getStatusCodeValue(), right.getStatusCodeValue()));
      Collections.sort(statuses);
      assertEquals(Arrays.asList(201, 409), statuses, left.getBody() + "\n" + right.getBody());
      JsonNode winner =
          mapper.readTree(left.getStatusCodeValue() == 201 ? left.getBody() : right.getBody());
      Map<String, Object> row = assertViewPersisted(database, VIEW, winner);
      assertEquals(0L, ((Number) row.get("version")).longValue());
      assertEquals(
          "creator_" + (left.getStatusCodeValue() == 201 ? 0 : 1),
          metadata(winner).properties().get("winner"));
      assertEquals(1, metadata(winner).history().size());
      assertEquals(
          pointer(winner),
          request(HttpMethod.GET, viewPath(database, VIEW), null, 200)
              .path("metadataLocation")
              .asText());
    } finally {
      start.countDown();
      executor.shutdownNow();
      assertTrue(executor.awaitTermination(30, TimeUnit.SECONDS));
    }
  }

  @Test
  void serverOwnedResponseAndMetadataFieldsCannotBeSuppliedByTheCaller() throws Exception {
    ObjectNode body = payload(database, VIEW);
    body.put("clusterId", "caller-cluster");
    body.put("viewCreator", "caller-creator");
    body.put("metadataLocation", "file:/caller-pointer-marker");
    body.put("viewVersion", "caller-version");
    body.put("creationTime", 1L);
    body.put("lastModifiedTime", 1L);
    JsonNode created = create(VIEW, body, 201);
    assertViewPersisted(database, VIEW, created);
    assertPointerShape(created);
    assertEquals(CREATOR, created.path("viewCreator").asText());
    assertNotEquals("file:/caller-pointer-marker", pointer(created));
    assertTrue(created.path("creationTime").asLong() > 1L);
    assertTrue(created.path("lastModifiedTime").asLong() > 1L);
    assertEquals(
        CREATOR, metadata(created).properties().get(getCanonicalFieldName("tableCreator")));
  }

  @Test
  void viewsOnlyDatabaseRemainsDiscoverableAfterDroppingItsLastTable() throws Exception {
    JsonNode created = create(VIEW, payload(database, VIEW), 201);
    request(HttpMethod.DELETE, tablesPath(database) + "/" + SEED_TABLE + "?purge=true", null, 204);
    assertFalse(fixtures.persistedEntity(database, SEED_TABLE).isPresent());
    JsonNode databasesResponse = request(HttpMethod.GET, "/v1/databases", null, 200);
    boolean found = false;
    for (JsonNode result : databasesResponse.path("results")) {
      found |= database.equals(result.path("databaseId").asText());
    }
    assertTrue(found, databasesResponse.toString());
    assertViewPersisted(
        database, VIEW, request(HttpMethod.GET, viewPath(database, VIEW), null, 200));
    assertEquals(
        1, request(HttpMethod.GET, collection(database), null, 200).path("results").size());
    assertEquals(
        pointer(created),
        request(HttpMethod.GET, viewPath(database, VIEW), null, 200)
            .path("metadataLocation")
            .asText());
  }

  @Test
  void omittedOptionalDefinitionFieldsUseEmptyMetadataDefaults() throws Exception {
    ObjectNode body = payload(database, VIEW);
    body.remove(Arrays.asList("defaultCatalog", "defaultNamespace", "viewProperties"));
    JsonNode created = create(VIEW, body, 201);
    assertViewPersisted(database, VIEW, created);
    ViewMetadata metadata = metadata(created);
    assertEquals(null, metadata.currentVersion().defaultCatalog());
    assertTrue(metadata.currentVersion().defaultNamespace().isEmpty());
    assertFalse(metadata.properties().containsKey("description"));
    Map<String, Object> before = assertViewPersisted(database, VIEW, created);
    body.put("baseMetadataLocation", pointer(created));
    JsonNode unchanged = request(HttpMethod.PUT, viewPath(database, VIEW), body, 200);
    assertEquals(before, assertViewPersisted(database, VIEW, unchanged));
  }

  private ObjectNode payload(String db, String view) throws Exception {
    ObjectNode body =
        (ObjectNode) mapper.readTree(ViewModelConstants.createRequestWithoutBaseVersion().toJson());
    body.put("databaseId", db);
    body.put("viewId", view);
    body.putArray("defaultNamespace").add(db);
    body.set("viewProperties", mapper.createObjectNode().put("description", "initial"));
    ((ObjectNode) body.withArray("representations").get(0)).put("sql", SQL);
    return body;
  }

  private ObjectNode tablePayload(String db, String table) {
    ObjectNode body =
        mapper
            .createObjectNode()
            .put("databaseId", db)
            .put("tableId", table)
            .put("clusterId", clusterProperties.getClusterName())
            .put("tableType", "PRIMARY_TABLE")
            .put("baseTableVersion", INITIAL_TABLE_VERSION)
            .put("schema", ViewModelConstants.VIEW_SCHEMA_LITERAL);
    body.putObject("tableProperties");
    return body;
  }

  private void seedDatabase(String db) throws Exception {
    databases.add(db);
    tableNames.add(SEED_TABLE);
    request(HttpMethod.POST, tablesPath(db), tablePayload(db, SEED_TABLE), 201);
    assertEquals(
        "TABLE", fixtures.persistedEntity(db, SEED_TABLE).orElseThrow().get("entity_type"));
  }

  private void enableViews(String db) {
    databases.add(db);
    TableToggleStatus toggle =
        TableToggleStatus.builder()
            .featureId("views")
            .databaseId(db.toLowerCase(Locale.ROOT))
            .tableId("*")
            .build();
    fixtures.seedToggle(toggle);
    toggles.add(toggle);
  }

  private JsonNode create(String view, ObjectNode body, int status) throws Exception {
    views.add(view);
    return request(HttpMethod.POST, collection(database), body, status);
  }

  private JsonNode request(HttpMethod method, String path, Object body, int status)
      throws Exception {
    ResponseEntity<String> response =
        tables.exchange(
            path,
            method,
            new HttpEntity<>(body == null ? null : body.toString(), headers),
            String.class);
    assertEquals(
        status, response.getStatusCodeValue(), method + " " + path + ": " + response.getBody());
    if (status == 204) {
      assertTrue(response.getBody() == null || response.getBody().isEmpty());
      return mapper.nullNode();
    }
    assertNotNull(response.getBody(), method + " " + path);
    return mapper.readTree(response.getBody());
  }

  private Map<String, Object> assertViewPersisted(String db, String view, JsonNode response)
      throws Exception {
    ResponseEntity<String> typed = htsRead("views", db, view);
    assertEquals(200, typed.getStatusCodeValue(), typed.getBody());
    ResponseEntity<String> neutral = htsRead("entities", db, view);
    assertEquals(200, neutral.getStatusCodeValue(), neutral.getBody());
    JsonNode entity = mapper.readTree(typed.getBody()).path("entity");
    assertEquals(entity, mapper.readTree(neutral.getBody()).path("entity"));
    assertEquals("VIEW", entity.path("entityType").asText());
    assertEquals(pointer(response), entity.path("metadataLocation").asText());
    assertEquals(404, htsRead("tables", db, view).getStatusCodeValue());
    Map<String, Object> row = fixtures.persistedEntity(db, view).orElseThrow();
    assertEquals("VIEW", row.get("entity_type"));
    assertEquals(pointer(response), row.get("metadata_location"));
    // The wire CAS token is the current pointer. The legacy table_version SQL column is unmapped.
    assertEquals(pointer(response), entity.path("tableVersion").asText());
    assertEquals(null, row.get("table_version"));
    assertEquals(entity.path("storageType").asText(), row.get("storage_type"));
    assertEquals(
        entity.path("creationTime").asLong(), ((Number) row.get("creation_time")).longValue());
    ViewMetadata metadata = metadata(response);
    assertEquals(metadata.uuid(), metadata.properties().get(getCanonicalFieldName("tableUUID")));
    assertTrue(pointer(response).contains(metadata.uuid()));
    assertEquals(
        db.toLowerCase(Locale.ROOT), row.get("database_id").toString().toLowerCase(Locale.ROOT));
    assertEquals(
        view.toLowerCase(Locale.ROOT), row.get("table_id").toString().toLowerCase(Locale.ROOT));
    return row;
  }

  private void assertAbsent(String db, String view) {
    assertFalse(fixtures.persistedEntity(db, view).isPresent());
    assertEquals(404, htsRead("entities", db, view).getStatusCodeValue());
    assertEquals(404, htsRead("views", db, view).getStatusCodeValue());
  }

  private ResponseEntity<String> htsRead(String kind, String db, String view) {
    return hts.getForEntity(htsUri(kind, db, view), String.class);
  }

  private URI htsUri(String kind, String db, String view) {
    return UriComponentsBuilder.fromHttpUrl(
            clusterProperties.getClusterHouseTablesBaseUri() + "/hts/" + kind)
        .queryParam("databaseId", db)
        .queryParam("tableId", view)
        .build()
        .encode()
        .toUri();
  }

  private void deleteFixture(String kind, String db, String id) {
    ResponseEntity<String> response =
        hts.exchange(htsUri(kind, db, id), HttpMethod.DELETE, HttpEntity.EMPTY, String.class);
    assertTrue(
        response.getStatusCodeValue() == 204 || response.getStatusCodeValue() == 404,
        "Fixture cleanup failed: " + response.getStatusCodeValue() + " " + response.getBody());
  }

  private ViewMetadata metadata(JsonNode response) throws Exception {
    URI location = URI.create(pointer(response));
    Path path = location.getScheme() == null ? Paths.get(pointer(response)) : Paths.get(location);
    return ViewMetadataParser.fromJson(Files.readString(path));
  }

  private long metadataFileCount() throws Exception {
    try (Stream<Path> files =
        Files.walk(Paths.get(clusterProperties.getClusterStorageRootPath()))) {
      return files.filter(path -> path.toString().endsWith(".metadata.json")).count();
    }
  }

  private void assertPointerShape(JsonNode response) {
    assertEquals(clusterProperties.getClusterName(), response.path("clusterId").asText());
    assertEquals(response.path("metadataLocation"), response.path("viewVersion"));
    for (String field :
        Arrays.asList(
            "schema",
            "representations",
            "viewProperties",
            "viewUUID",
            "sourceDialect",
            "defaultNamespace",
            "defaultCatalog",
            "history")) {
      assertFalse(response.has(field), "Pointer response must not expose " + field);
    }
  }

  private void assertIdentifierShape(JsonNode response) {
    assertEquals(8, response.size());
    assertTrue(response.hasNonNull("databaseId"));
    assertTrue(response.hasNonNull("viewId"));
    for (String field :
        Arrays.asList("clusterId", "metadataLocation", "viewVersion", "viewCreator")) {
      assertFalse(response.hasNonNull(field), "Identifier listing must not expose " + field);
    }
    assertEquals(0, response.path("creationTime").asLong());
    assertEquals(0, response.path("lastModifiedTime").asLong());
  }

  private static String sql(ViewMetadata metadata) {
    return ((SQLViewRepresentation) metadata.currentVersion().representations().get(0)).sql();
  }

  private static String pointer(JsonNode response) {
    return response.path("metadataLocation").asText();
  }

  private static String collection(String db) {
    return "/v1/databases/" + db + "/views";
  }

  private static String viewPath(String db, String view) {
    return collection(db) + "/" + view;
  }

  private static String tablesPath(String db) {
    return "/v1/databases/" + db + "/tables";
  }

  private static String continuation(String db, String token, int size) {
    return UriComponentsBuilder.fromPath(collection(db))
        .queryParam("size", size)
        .queryParam("pageToken", token)
        .build()
        .encode()
        .toUriString();
  }
}

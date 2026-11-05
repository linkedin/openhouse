package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.gen.client.ssl.WebClientFactory;
import com.linkedin.openhouse.gen.tables.client.api.NamespaceApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataException;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableMetadataParser;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Runs against the packaged runtime in both Iceberg versions. */
class NamespaceMetadataTest {
  private MockWebServer tableService;
  private OpenHouseCatalog catalog;
  private Table table;
  private Map<String, String> tableProperties;

  @BeforeEach
  void setup() throws IOException {
    tableService = new MockWebServer();
    tableService.start();
    catalog = new OpenHouseCatalog();
    Map<String, String> configuration = new HashMap<>();
    configuration.put(CatalogProperties.URI, tableService.url("/").toString());
    configuration.put(OpenHouseCatalog.AUTH_TOKEN, "token");
    configuration.put(OpenHouseCatalog.CLIENT_NAME, "namespace-tests");
    configuration.put("header.X-OpenHouse-Example", "value");
    configuration.put(CatalogProperties.APP_ID, "namespace-session");
    catalog.initialize("openhouse.local", configuration);
    tableProperties = new HashMap<>();
    tableProperties.put("shared", "table");
    tableProperties.put("table-only", "value");
    table = mock(Table.class);
    when(table.name()).thenReturn("openhouse.local.database.table");
    when(table.properties()).thenReturn(tableProperties);
  }

  @AfterEach
  void teardown() throws IOException {
    tableService.shutdown();
  }

  @Test
  void effectivePropertiesInheritOverrideAndCopyWithoutCaching() throws Exception {
    respond(
        200,
        "{\"namespace\":[\"database\"],\"properties\":{\"shared\":\"database\",\"database-only\":\"inherited\"}}");
    Map<String, String> effective = catalog.getEffectiveTableProperties(table);
    assertEquals("table", effective.get("shared"));
    assertEquals("inherited", effective.get("database-only"));
    assertEquals("value", effective.get("table-only"));
    assertEquals(2, table.properties().size());
    assertFalse(table.properties().containsKey("database-only"));
    assertThrows(UnsupportedOperationException.class, () -> effective.put("other", "value"));
    tableProperties.put("shared", "changed");
    assertEquals("table", effective.get("shared"));
    assertEquals(1, tableService.getRequestCount());
    RecordedRequest request = tableService.takeRequest(5, TimeUnit.SECONDS);
    assertNotNull(request);
    assertEquals("GET", request.getMethod());
    assertEquals("/v1/openhouse.local/namespaces/database", request.getPath());
    assertEquals("Bearer token", request.getHeader("Authorization"));
    assertEquals("namespace-tests", request.getHeader(WebClientFactory.HTTP_HEADER_CLIENT_NAME));
    assertEquals("namespace-session", request.getHeader(WebClientFactory.HTTP_HEADER_SESSION_ID));
    assertEquals("value", request.getHeader("X-OpenHouse-Example"));
    respond(
        200,
        "{\"namespace\":[\"database\"],\"properties\":{\"shared\":\"database\",\"database-only\":\"fresh\"}}");
    assertEquals("fresh", catalog.getEffectiveTableProperties(table).get("database-only"));
    assertEquals(2, tableService.getRequestCount());
  }

  @Test
  void namespaceRequestsUseRefreshedCatalogToken() throws Exception {
    catalog.updateAuthToken("refreshed-token");
    respond(200, "{\"namespace\":[\"database\"],\"properties\":{}}");
    catalog.getEffectiveTableProperties(table);
    assertEquals(
        "Bearer refreshed-token",
        tableService.takeRequest(5, TimeUnit.SECONDS).getHeader("Authorization"));
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(ints = {200, 404})
  void emptyOrAbsentDatabaseMetadataPreservesActualProperties(int status) throws Exception {
    respond(status, "{\"namespace\":[\"database\"],\"properties\":{}}");
    Map<String, String> effective = catalog.getEffectiveTableProperties(table);
    assertEquals(tableProperties, effective);
    assertNotSame(tableProperties, effective);
    assertThrows(UnsupportedOperationException.class, () -> effective.clear());
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void emptyTablePropertiesInheritDatabaseMetadata() throws Exception {
    tableProperties.clear();
    respond(200, "{\"namespace\":[\"DATABASE\"],\"properties\":{\"database-only\":\"inherited\"}}");
    assertEquals(
        Collections.singletonMap("database-only", "inherited"),
        catalog.getEffectiveTableProperties(table));
    assertTrue(tableProperties.isEmpty());
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"namespace\":[\"database\"]}",
        "{\"namespace\":[\"database\"],\"properties\":null}"
      })
  void optionalPropertiesDefaultToEmpty(String body) throws Exception {
    respond(200, body);
    assertEquals(tableProperties, catalog.getEffectiveTableProperties(table));
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 401, 403, 409, 422, 500, 503})
  void effectivePropertiesPropagatesEveryNon404Failure(int status) {
    respond(
        status,
        "{\"error\":{\"message\":\"failure\",\"type\":\"ServiceException\",\"code\":"
            + status
            + "}}");
    NamespaceMetadataException failure =
        assertThrows(
            NamespaceMetadataException.class, () -> catalog.getEffectiveTableProperties(table));
    assertEquals(status, failure.getStatusCode());
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void transportFailureIsCheckedAndDoesNotRetry() {
    tableService.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST));
    respond(503, "{}");
    NamespaceMetadataException failure =
        assertThrows(
            NamespaceMetadataException.class, () -> catalog.getEffectiveTableProperties(table));
    assertEquals(0, failure.getStatusCode());
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "null",
        "{",
        "{}",
        "{\"namespace\":[\"database\"],\"properties\":{\"key\":null}}",
        "{\"namespace\":[\"database\"],\"properties\":{\"key\":12}}",
        "{\"namespace\":[\"database\"],\"properties\":[]}",
        "{\"namespace\":[\"other\"],\"properties\":{}}",
        "{\"namespace\":[\"database\",\"other\"],\"properties\":{}}",
        "{\"namespace\":[null],\"properties\":{}}"
      })
  void malformedMetadataFailsRatherThanFallingBack(String body) {
    respond(200, body);
    assertEquals(
        0,
        assertThrows(
                NamespaceMetadataException.class, () -> catalog.getEffectiveTableProperties(table))
            .getStatusCode());
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "database.table",
        "foreign.database.table",
        "openhouse.locality.database.table",
        "openhouse.local.database.table.extra",
        "openhouse.local..table",
        "openhouse.local.database.",
        "openhouse.local.database.table/name",
        "openhouse.local.database.table name",
        "openhouse.local.database",
        "",
        "openhouse.local.database%2Fother.table"
      })
  void invalidOrForeignTableNamesFailBeforeRequest(String name) {
    when(table.name()).thenReturn(name);
    assertThrows(
        NamespaceMetadataException.class, () -> catalog.getEffectiveTableProperties(table));
    assertEquals(0, tableService.getRequestCount());
  }

  @Test
  void absentTableNameFailsBeforeRequest() {
    when(table.name()).thenReturn(null);
    assertThrows(
        NamespaceMetadataException.class, () -> catalog.getEffectiveTableProperties(table));
    assertEquals(0, tableService.getRequestCount());
  }

  @Test
  void prefixIsEncodedAsOnePathSegment() throws Exception {
    catalog.initialize(
        "catalog/with space",
        Collections.singletonMap(CatalogProperties.URI, tableService.url("/").toString()));
    when(table.name()).thenReturn("catalog/with space.database.table");
    respond(200, "{\"namespace\":[\"database\"],\"properties\":{}}");
    catalog.getEffectiveTableProperties(table);
    assertEquals(
        "/v1/catalog%2Fwith%20space/namespaces/database",
        tableService.takeRequest(5, TimeUnit.SECONDS).getPath());
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void namespaceOperationsUseGeneratedRequestsAndReportChanges() throws Exception {
    Namespace namespace = Namespace.of("database");
    respond(200, "{\"namespace\":[\"database\"],\"properties\":{\"key\":\"value\"}}");
    catalog.createNamespace(namespace, Collections.singletonMap("key", "value"));
    RecordedRequest create = tableService.takeRequest(5, TimeUnit.SECONDS);
    assertEquals("POST", create.getMethod());
    assertEquals("/v1/openhouse.local/namespaces", create.getPath());
    JsonNode createBody = new ObjectMapper().readTree(create.getBody().readUtf8());
    assertEquals("database", createBody.path("namespace").get(0).asText());
    assertEquals("value", createBody.path("properties").path("key").asText());

    respond(200, "{\"namespace\":[\"database\"],\"properties\":{\"key\":\"value\"}}");
    Map<String, String> loaded = catalog.loadNamespaceMetadata(namespace);
    assertEquals(Collections.singletonMap("key", "value"), loaded);
    assertThrows(UnsupportedOperationException.class, loaded::clear);
    assertEquals("GET", tableService.takeRequest(5, TimeUnit.SECONDS).getMethod());

    respond(200, "{\"updated\":[\"key\"],\"removed\":[],\"missing\":[]}");
    assertTrue(catalog.setProperties(namespace, Collections.singletonMap("key", "changed")));
    RecordedRequest update = tableService.takeRequest(5, TimeUnit.SECONDS);
    assertEquals("POST", update.getMethod());
    assertEquals("/v1/openhouse.local/namespaces/database/properties", update.getPath());
    JsonNode updateBody = new ObjectMapper().readTree(update.getBody().readUtf8());
    assertEquals("changed", updateBody.path("updates").path("key").asText());
    assertTrue(updateBody.path("removals").isEmpty());

    respond(200, "{\"updated\":[],\"removed\":[\"key\"],\"missing\":[]}");
    assertTrue(catalog.removeProperties(namespace, Collections.singleton("key")));
    JsonNode removeBody =
        new ObjectMapper()
            .readTree(tableService.takeRequest(5, TimeUnit.SECONDS).getBody().readUtf8());
    assertTrue(removeBody.path("updates").isEmpty());
    assertEquals("key", removeBody.path("removals").get(0).asText());

    respond(200, "{\"updated\":[],\"removed\":[],\"missing\":[\"key\"]}");
    assertFalse(catalog.removeProperties(namespace, Collections.singleton("key")));
    respond(200, "{\"updated\":[],\"removed\":[],\"missing\":[]}");
    assertFalse(catalog.setProperties(namespace, Collections.emptyMap()));
    tableService.takeRequest(5, TimeUnit.SECONDS);
    tableService.takeRequest(5, TimeUnit.SECONDS);

    respond(204, "");
    assertTrue(catalog.dropNamespace(namespace));
    RecordedRequest drop = tableService.takeRequest(5, TimeUnit.SECONDS);
    assertEquals("DELETE", drop.getMethod());
    assertEquals("/v1/openhouse.local/namespaces/database", drop.getPath());
    respond(404, "{}");
    assertFalse(catalog.dropNamespace(namespace));
    assertEquals(8, tableService.getRequestCount());
  }

  @Test
  void createWithoutPropertiesSendsEmptyMap() throws Exception {
    respond(200, "{\"namespace\":[\"database\"]}");
    catalog.createNamespace(Namespace.of("database"));
    assertTrue(
        new ObjectMapper()
            .readTree(tableService.takeRequest(5, TimeUnit.SECONDS).getBody().readUtf8())
            .path("properties")
            .isEmpty());
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"updated\":[],\"removed\":[\"foo\"]}",
        "{\"updated\":[],\"removed\":[\"foo\"],\"missing\":null}",
        "{\"updated\":[],\"removed\":[\"foo\"],\"missing\":[\"bar\"]}"
      })
  void optionalMissingKeysPermitPartialRemovalAcknowledgement(String body) {
    respond(200, body);
    assertTrue(
        catalog.removeProperties(
            Namespace.of("database"), new HashSet<>(Arrays.asList("foo", "bar"))));
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void optionalMissingKeysPermitNoRemovalAcknowledgement() {
    respond(200, "{\"updated\":[],\"removed\":[]}");
    assertFalse(catalog.removeProperties(Namespace.of("database"), Collections.singleton("foo")));
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @MethodSource("namespaceFailures")
  void namespaceOverridesTranslateFailures(
      String operation, int status, Class<? extends RuntimeException> expected) {
    respond(status, "{}");
    assertThrows(
        expected,
        () -> {
          switch (operation) {
            case "create":
              catalog.createNamespace(Namespace.of("database"));
              break;
            case "load":
              catalog.loadNamespaceMetadata(Namespace.of("database"));
              break;
            case "drop":
              catalog.dropNamespace(Namespace.of("database"));
              break;
            case "remove":
              catalog.removeProperties(Namespace.of("database"), Collections.singleton("key"));
              break;
            default:
              catalog.setProperties(
                  Namespace.of("database"), Collections.singletonMap("key", "value"));
              break;
          }
        });
    assertEquals(1, tableService.getRequestCount());
  }

  static Stream<Arguments> namespaceFailures() {
    return Stream.of(
        Arguments.of("create", 409, AlreadyExistsException.class),
        Arguments.of("create", 400, ValidationException.class),
        Arguments.of("create", 401, NotAuthorizedException.class),
        Arguments.of("create", 403, ForbiddenException.class),
        Arguments.of("load", 404, NoSuchNamespaceException.class),
        Arguments.of("load", 500, RESTException.class),
        Arguments.of("load", 503, RESTException.class),
        Arguments.of("drop", 409, NamespaceNotEmptyException.class),
        Arguments.of("drop", 403, ForbiddenException.class),
        Arguments.of("set", 404, NoSuchNamespaceException.class),
        Arguments.of("set", 409, CommitFailedException.class),
        Arguments.of("set", 422, ValidationException.class),
        Arguments.of("remove", 404, NoSuchNamespaceException.class),
        Arguments.of("remove", 409, CommitFailedException.class));
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "",
        "{}",
        "{\"updated\":[],\"removed\":[],\"missing\":12}",
        "{\"updated\":[null],\"removed\":[],\"missing\":[]}"
      })
  void malformedUpdateFails(String body) {
    respond(200, body);
    assertThrows(
        RESTException.class,
        () -> catalog.setProperties(Namespace.of("database"), Collections.emptyMap()));
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"updated\":[\"other\"],\"removed\":[],\"missing\":[]}",
        "{\"updated\":[],\"removed\":[],\"missing\":[]}",
        "{\"updated\":[\"key\",\"key\"],\"removed\":[],\"missing\":[]}",
        "{\"updated\":[\"key\"],\"removed\":[\"other\"],\"missing\":[]}"
      })
  void inconsistentUpdateAcknowledgementFails(String body) {
    respond(200, body);
    assertThrows(
        RESTException.class,
        () ->
            catalog.setProperties(
                Namespace.of("database"), Collections.singletonMap("key", "value")));
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"updated\":[],\"removed\":[\"bar\"]}",
        "{\"updated\":[],\"removed\":[\"foo\",\"foo\"]}",
        "{\"updated\":[],\"removed\":[\"foo\"],\"missing\":[\"foo\"]}"
      })
  void inconsistentRemovalAcknowledgementFails(String body) {
    respond(200, body);
    assertThrows(
        RESTException.class,
        () -> catalog.removeProperties(Namespace.of("database"), Collections.singleton("foo")));
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void invalidPropertyValuesFailBeforeRequest() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", null);
    assertThrows(
        ValidationException.class,
        () -> catalog.createNamespace(Namespace.of("database"), properties));
    assertThrows(
        ValidationException.class,
        () -> catalog.setProperties(Namespace.of("database"), properties));
    assertThrows(
        ValidationException.class,
        () -> catalog.removeProperties(Namespace.of("database"), Collections.singleton(null)));
    assertEquals(0, tableService.getRequestCount());
  }

  @Test
  void unexpectedSuccessStatusFails() {
    respond(201, "{\"namespace\":[\"database\"],\"properties\":{}}");
    assertThrows(RESTException.class, () -> catalog.createNamespace(Namespace.of("database")));
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void invalidNamespaceFailsBeforeRequestAndExistenceRemainsUnsupported() {
    assertThrows(ValidationException.class, () -> catalog.loadNamespaceMetadata(Namespace.empty()));
    assertThrows(
        ValidationException.class, () -> catalog.createNamespace(Namespace.of("parent", "child")));
    assertThrows(
        ValidationException.class, () -> catalog.dropNamespace(Namespace.of("with/slash")));
    assertThrows(
        UnsupportedOperationException.class,
        () -> catalog.namespaceExists(Namespace.of("database")));
    assertEquals(0, tableService.getRequestCount());
  }

  @Test
  void generatedNamespaceApiIsPackaged() {
    respond(200, "{\"namespace\":[\"database\"],\"properties\":{\"key\":\"value\"}}");
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(tableService.url("/").toString());
    assertEquals(
        "value",
        new NamespaceApi(apiClient)
            .loadNamespaceMetadataV1("openhouse.local", "database")
            .block()
            .getProperties()
            .get("key"));
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void actualLoadTablePreservesPropertiesAndMakesOnlyTableRequest() throws Exception {
    Path metadataDirectory = Paths.get("build", "namespace-metadata");
    Files.createDirectories(metadataDirectory);
    Path metadataPath =
        metadataDirectory.resolve(UUID.randomUUID() + ".metadata.json").toAbsolutePath();
    TableMetadata metadata =
        TableMetadata.newTableMetadata(
            new Schema(Types.NestedField.required(1, "id", Types.LongType.get())),
            PartitionSpec.unpartitioned(),
            metadataDirectory.toAbsolutePath().toString(),
            Collections.singletonMap("table-only", "value"));
    TableMetadataParser.write(
        metadata, org.apache.iceberg.Files.localOutput(metadataPath.toFile()));
    FileIO fileOperations = mock(FileIO.class);
    when(fileOperations.newInputFile(metadataPath.toString()))
        .thenReturn(org.apache.iceberg.Files.localInput(metadataPath.toFile()));
    OpenHouseCatalog loadingCatalog =
        new OpenHouseCatalog() {
          @Override
          protected FileIO loadFileIO(Map<String, String> properties) {
            return fileOperations;
          }
        };
    loadingCatalog.initialize(
        "openhouse.local",
        Collections.singletonMap(CatalogProperties.URI, tableService.url("/").toString()));
    respond(200, "{\"tableLocation\":\"" + metadataPath + "\",\"config\":{}}");
    Table loaded = loadingCatalog.loadTable(TableIdentifier.of("database", "table"));
    assertEquals("openhouse.local.database.table", loaded.name());
    assertEquals("value", loaded.properties().get("table-only"));
    assertEquals(1, tableService.getRequestCount());
    assertEquals(
        "/v1/databases/database/tables/table",
        tableService.takeRequest(5, TimeUnit.SECONDS).getPath());
    respond(200, "{\"namespace\":[\"database\"],\"properties\":{\"database-only\":\"inherited\"}}");
    assertEquals(
        "inherited", loadingCatalog.getEffectiveTableProperties(loaded).get("database-only"));
    assertFalse(loaded.properties().containsKey("database-only"));
    assertEquals(2, tableService.getRequestCount());
  }

  @Test
  void legacyListingKeepsItsRoute() throws Exception {
    respond(200, "{\"results\":[{\"databaseId\":\"database\"}]}");
    assertEquals(Collections.singletonList(Namespace.of("database")), catalog.listNamespaces());
    assertEquals("/v1/databases", tableService.takeRequest(5, TimeUnit.SECONDS).getPath());
    assertEquals(1, tableService.getRequestCount());
  }

  private void respond(int status, String body) {
    tableService.enqueue(
        new MockResponse()
            .setResponseCode(status)
            .setHeader("Content-Type", "application/json")
            .setBody(body));
  }
}

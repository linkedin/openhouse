package com.linkedin.openhouse.javaclient;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.linkedin.openhouse.gen.tables.client.api.NamespaceApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.JsonNode;
import com.linkedin.openhouse.relocated.com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpStatus;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/** Runs against the packaged Iceberg 1.5 runtime. */
class NamespaceMetadataTest {
  private MockWebServer tableService;
  private OpenHouseCatalog catalog;

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
  }

  @AfterEach
  void teardown() throws IOException {
    tableService.shutdown();
  }

  @Test
  void namespaceRequestsUseRefreshedCatalogToken() throws Exception {
    catalog.updateAuthToken("refreshed-token");
    respond(200, "{\"namespace\":[\"namespace\"],\"properties\":{}}");
    catalog.loadNamespaceMetadata(Namespace.of("namespace"));
    assertEquals(
        "Bearer refreshed-token",
        tableService.takeRequest(5, TimeUnit.SECONDS).getHeader("Authorization"));
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void maximumLengthNamespaceLoadsMetadata() {
    String identifier = String.join("", Collections.nCopies(128, "a"));
    respond(200, "{\"namespace\":[\"" + identifier + "\"],\"properties\":{}}");
    assertTrue(catalog.loadNamespaceMetadata(Namespace.of(identifier)).isEmpty());
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void overlengthNamespaceFailsBeforeRequests() {
    Namespace namespace = Namespace.of(String.join("", Collections.nCopies(129, "a")));
    assertThrows(ValidationException.class, () -> catalog.createNamespace(namespace));
    assertThrows(ValidationException.class, () -> catalog.loadNamespaceMetadata(namespace));
    assertThrows(ValidationException.class, () -> catalog.dropNamespace(namespace));
    assertThrows(
        ValidationException.class, () -> catalog.setProperties(namespace, Collections.emptyMap()));
    assertThrows(
        ValidationException.class,
        () -> catalog.removeProperties(namespace, Collections.emptySet()));
    assertEquals(0, tableService.getRequestCount());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "{\"namespace\":[\"namespace\"]}",
        "{\"namespace\":[\"namespace\"],\"properties\":null}"
      })
  void optionalPropertiesDefaultToEmpty(String body) throws Exception {
    respond(200, body);
    assertTrue(catalog.loadNamespaceMetadata(Namespace.of("namespace")).isEmpty());
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void invalidNamespaceDiagnosticSurvivesCatalogTranslation() {
    assertTrue(
        assertThrows(
                ValidationException.class,
                () -> catalog.loadNamespaceMetadata(Namespace.of("foo.bar")))
            .getMessage()
            .contains("'foo.bar'"));
    assertEquals(0, tableService.getRequestCount());
  }

  @Test
  void prefixIsEncodedAsOnePathSegment() throws Exception {
    catalog.initialize(
        "catalog/with space",
        Collections.singletonMap(CatalogProperties.URI, tableService.url("/").toString()));
    respond(200, "{\"namespace\":[\"namespace\"],\"properties\":{}}");
    catalog.loadNamespaceMetadata(Namespace.of("namespace"));
    assertEquals(
        "/v1/catalog%2Fwith%20space/namespaces/namespace",
        tableService.takeRequest(5, TimeUnit.SECONDS).getPath());
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void namespaceOperationsUseGeneratedRequestsAndReportChanges() throws Exception {
    Namespace namespace = Namespace.of("namespace");
    respond(200, "{\"namespace\":[\"namespace\"],\"properties\":{\"key\":\"value\"}}");
    catalog.createNamespace(namespace, Collections.singletonMap("key", "value"));
    RecordedRequest create = tableService.takeRequest(5, TimeUnit.SECONDS);
    assertEquals("POST", create.getMethod());
    assertEquals("/v1/openhouse.local/namespaces", create.getPath());
    JsonNode createBody = new ObjectMapper().readTree(create.getBody().readUtf8());
    assertEquals("namespace", createBody.path("namespace").get(0).asText());
    assertEquals("value", createBody.path("properties").path("key").asText());

    respond(200, "{\"namespace\":[\"namespace\"],\"properties\":{\"key\":\"value\"}}");
    Map<String, String> loaded = catalog.loadNamespaceMetadata(namespace);
    assertEquals(Collections.singletonMap("key", "value"), loaded);
    assertThrows(UnsupportedOperationException.class, loaded::clear);
    assertEquals("GET", tableService.takeRequest(5, TimeUnit.SECONDS).getMethod());

    respond(200, "{\"updated\":[\"key\"],\"removed\":[],\"missing\":[]}");
    assertTrue(catalog.setProperties(namespace, Collections.singletonMap("key", "changed")));
    RecordedRequest update = tableService.takeRequest(5, TimeUnit.SECONDS);
    assertEquals("POST", update.getMethod());
    assertEquals("/v1/openhouse.local/namespaces/namespace/properties", update.getPath());
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
    assertEquals("/v1/openhouse.local/namespaces/namespace", drop.getPath());
    respond(404, "{}");
    assertFalse(catalog.dropNamespace(namespace));
    assertEquals(8, tableService.getRequestCount());
  }

  @Test
  void createWithoutPropertiesSendsEmptyMap() throws Exception {
    respond(200, "{\"namespace\":[\"namespace\"]}");
    catalog.createNamespace(Namespace.of("namespace"));
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
            Namespace.of("namespace"), new HashSet<>(Arrays.asList("foo", "bar"))));
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void optionalMissingKeysPermitNoRemovalAcknowledgement() {
    respond(200, "{\"updated\":[],\"removed\":[]}");
    assertFalse(catalog.removeProperties(Namespace.of("namespace"), Collections.singleton("foo")));
    assertEquals(1, tableService.getRequestCount());
  }

  @ParameterizedTest
  @MethodSource("namespaceFailures")
  void namespaceOverridesTranslateFailures(
      String operation, HttpStatus status, Class<? extends RuntimeException> expected) {
    respond(status.value(), "{}");
    RuntimeException failure =
        assertThrows(
            expected,
            () -> {
              switch (operation) {
                case "create":
                  catalog.createNamespace(Namespace.of("namespace"));
                  break;
                case "load":
                  catalog.loadNamespaceMetadata(Namespace.of("namespace"));
                  break;
                case "drop":
                  catalog.dropNamespace(Namespace.of("namespace"));
                  break;
                case "remove":
                  catalog.removeProperties(Namespace.of("namespace"), Collections.singleton("key"));
                  break;
                case "set":
                  catalog.setProperties(
                      Namespace.of("namespace"), Collections.singletonMap("key", "value"));
                  break;
                default:
                  fail("Unknown namespace operation: " + operation);
                  break;
              }
            });
    assertEquals(expected, failure.getClass());
    assertEquals(1, tableService.getRequestCount());
  }

  static Stream<Arguments> namespaceFailures() {
    return Stream.of(
        Arguments.of("create", HttpStatus.CONFLICT, AlreadyExistsException.class),
        Arguments.of("create", HttpStatus.BAD_REQUEST, ValidationException.class),
        Arguments.of("create", HttpStatus.UNAUTHORIZED, NotAuthorizedException.class),
        Arguments.of("create", HttpStatus.FORBIDDEN, ForbiddenException.class),
        Arguments.of("load", HttpStatus.NOT_FOUND, NoSuchNamespaceException.class),
        Arguments.of("load", HttpStatus.CONFLICT, RESTException.class),
        Arguments.of("load", HttpStatus.INTERNAL_SERVER_ERROR, RESTException.class),
        Arguments.of("load", HttpStatus.SERVICE_UNAVAILABLE, RESTException.class),
        Arguments.of("drop", HttpStatus.BAD_REQUEST, NamespaceNotEmptyException.class),
        Arguments.of("drop", HttpStatus.CONFLICT, CommitFailedException.class),
        Arguments.of("drop", HttpStatus.FORBIDDEN, ForbiddenException.class),
        Arguments.of("set", HttpStatus.NOT_FOUND, NoSuchNamespaceException.class),
        Arguments.of("set", HttpStatus.BAD_REQUEST, ValidationException.class),
        Arguments.of("set", HttpStatus.CONFLICT, CommitFailedException.class),
        Arguments.of("set", HttpStatus.UNPROCESSABLE_ENTITY, ValidationException.class),
        Arguments.of("remove", HttpStatus.NOT_FOUND, NoSuchNamespaceException.class),
        Arguments.of("remove", HttpStatus.CONFLICT, CommitFailedException.class));
  }

  @Test
  void invalidPropertyValuesFailBeforeRequest() {
    Map<String, String> properties = new HashMap<>();
    properties.put("key", null);
    assertThrows(
        ValidationException.class,
        () -> catalog.createNamespace(Namespace.of("namespace"), properties));
    assertThrows(
        ValidationException.class,
        () -> catalog.setProperties(Namespace.of("namespace"), properties));
    assertThrows(
        ValidationException.class,
        () -> catalog.removeProperties(Namespace.of("namespace"), Collections.singleton(null)));
    assertEquals(0, tableService.getRequestCount());
  }

  @Test
  void lostMutationResponseRequiresReconciliationAndIsSentOnce() {
    tableService.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AFTER_REQUEST));
    respond(503, "{}");
    RESTException failure =
        assertThrows(RESTException.class, () -> catalog.createNamespace(Namespace.of("namespace")));
    assertNotNull(failure.getCause());
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void connectionResetDoesNotResendMutation() {
    tableService.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.DISCONNECT_AT_START));
    respond(200, "{\"updated\":[\"key\"],\"removed\":[],\"missing\":[]}");
    assertThrows(
        RESTException.class,
        () -> catalog.setProperties(Namespace.of("namespace"), Map.of("key", "value")));
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
        () -> catalog.namespaceExists(Namespace.of("namespace")));
    assertEquals(0, tableService.getRequestCount());
  }

  @Test
  void generatedNamespaceApiIsPackaged() {
    respond(200, "{\"namespace\":[\"namespace\"],\"properties\":{\"key\":\"value\"}}");
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(tableService.url("/").toString());
    assertEquals(
        "value",
        new NamespaceApi(apiClient)
            .loadNamespaceMetadataV1("openhouse.local", "namespace")
            .block()
            .getProperties()
            .get("key"));
    assertEquals(1, tableService.getRequestCount());
  }

  @Test
  void legacyListingKeepsItsRoute() throws Exception {
    respond(200, "{\"results\":[{\"databaseId\":\"namespace\"}]}");
    assertEquals(Collections.singletonList(Namespace.of("namespace")), catalog.listNamespaces());
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

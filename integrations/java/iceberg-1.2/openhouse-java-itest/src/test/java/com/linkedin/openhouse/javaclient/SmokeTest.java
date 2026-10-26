package com.linkedin.openhouse.javaclient;

import com.google.common.collect.ImmutableMap;
import com.linkedin.openhouse.gen.client.ssl.WebClientFactory;
import com.linkedin.openhouse.gen.tables.client.api.DatabaseApi;
import com.linkedin.openhouse.gen.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.gen.tables.client.model.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException;
import com.linkedin.openhouse.relocated.reactor.core.publisher.Mono;
import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.metrics.MetricsReporter;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * This class tests out packaging done in :integrations:java:iceberg-1.2:openhouse-java-runtime. The
 * goal of this test is to ensure all of `tableclient` functionality, such as configuring {@link
 * ApiClient}, {@link TableApi}, making REST call can be satisfied by the singular jar
 * `openhouse-java-runtime.jar`. These tests do not test complete functionality, rather it tests
 * various interfaces and their integration.
 */
public class SmokeTest {

  private static MockWebServer mockTableService = null;
  private static String url = null;

  @BeforeEach
  void setup() throws IOException {
    mockTableService = new MockWebServer();
    mockTableService.start();
    url = String.format("http://%s:%s", mockTableService.getHostName(), mockTableService.getPort());
  }

  @AfterEach
  void teardown() throws IOException {
    mockTableService.shutdown();
  }

  @Test
  public void testTableApiInterfacesWorking() {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(404).addHeader("Content-Type", "application/json"));

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(url);
    TableApi tableApi = new TableApi(apiClient);
    Assertions.assertDoesNotThrow(
        () ->
            tableApi
                .getTableV1("database", "table")
                .onErrorResume(WebClientResponseException.NotFound.class, e -> Mono.empty())
                .block());
  }

  @Test
  public void testTableApiDefaultHeadersPropagated() throws InterruptedException {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json"));

    String expectedHeader = WebClientFactory.HTTP_HEADER_CLIENT_NAME;
    String expectedHeaderValue = "anyvalue";

    ApiClient apiClient = new ApiClient();
    apiClient.addDefaultHeader(expectedHeader, expectedHeaderValue);
    apiClient.setBasePath(url);
    new TableApi(apiClient).getTableV1("database", "table").block();
    Assertions.assertEquals(
        expectedHeaderValue, mockTableService.takeRequest().getHeader(expectedHeader));
  }

  @Test
  public void testCatalogClientNamePropagated() throws InterruptedException {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json"));
    OpenHouseCatalog openHouseCatalog = new OpenHouseCatalog();
    String expectedHeader =
        WebClientFactory
            .HTTP_HEADER_CLIENT_NAME; // this value is a constant hidden in webclientfactory
    String expectedHeaderValue = "anyvalue";
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, url);
    properties.put("auth-token", "token");
    properties.put(OpenHouseCatalog.CLIENT_NAME, expectedHeaderValue);
    openHouseCatalog.initialize("openhouse", properties);
    openHouseCatalog.tableExists(TableIdentifier.of("db", "table"));
    Assertions.assertEquals(
        expectedHeaderValue, mockTableService.takeRequest().getHeader(expectedHeader));
  }

  @ParameterizedTest
  @CsvSource(
      value = {"SYSTEM,SYSTEM", "system,SYSTEM", "NULL,NULL"},
      nullValues = "NULL")
  public void testActionTypeHeader(String actionType, String expected) throws InterruptedException {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json"));
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, url);
    if (actionType != null) {
      properties.put("action-type", actionType);
    }
    OpenHouseCatalog catalog = new OpenHouseCatalog();
    catalog.initialize("openhouse", properties);
    catalog.tableExists(TableIdentifier.of("db", "table"));
    RecordedRequest request = mockTableService.takeRequest(5, TimeUnit.SECONDS);
    Assertions.assertNotNull(request);
    Assertions.assertEquals(expected, request.getHeader("X-OpenHouse-Action-Type"));
    Assertions.assertNull(request.getHeader("X-OpenHouse-System-Action"));
  }

  @ParameterizedTest
  @ValueSource(strings = {"invalid", "", " SYSTEM ", "true", "false", "USER", "uSeR"})
  public void testInvalidActionTypeRejected(String actionType) {
    IllegalArgumentException failure =
        Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                new OpenHouseCatalog()
                    .initialize(
                        "openhouse",
                        ImmutableMap.of(CatalogProperties.URI, url, "action-type", actionType)));
    Assertions.assertEquals("action-type must be SYSTEM when supplied", failure.getMessage());
  }

  @Test
  public void testConfiguredHeadersAreCatalogScoped() throws InterruptedException {
    String header = "X-OpenHouse-Example";
    OpenHouseCatalog configured = new OpenHouseCatalog();
    configured.initialize(
        "configured", ImmutableMap.of(CatalogProperties.URI, url, "header." + header, "value"));
    OpenHouseCatalog plain = new OpenHouseCatalog();
    plain.initialize("plain", ImmutableMap.of(CatalogProperties.URI, url));
    for (OpenHouseCatalog catalog : new OpenHouseCatalog[] {configured, plain}) {
      mockTableService.enqueue(new MockResponse().setResponseCode(404));
      Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("db", "table")));
      Assertions.assertEquals(
          catalog == configured ? "value" : null,
          mockTableService.takeRequest(1, TimeUnit.SECONDS).getHeader(header));
    }
  }

  /**
   * The Java catalog declares no capability, an engine's catalog declares its own, and a configured
   * value adds to either.
   */
  @Test
  public void testCatalogDeclaresCapabilities() throws InterruptedException {
    String header = "X-OpenHouse-Client-Capabilities";
    OpenHouseCatalog plain = new OpenHouseCatalog();
    plain.initialize("plain", ImmutableMap.of(CatalogProperties.URI, url));
    OpenHouseCatalog configured = new OpenHouseCatalog();
    configured.initialize(
        "configured",
        ImmutableMap.of(
            CatalogProperties.URI, url, "header." + header, " V3, column-defaults,,branching"));
    OpenHouseCatalog engine = new ColumnDefaultsEngineCatalog();
    engine.initialize("engine", ImmutableMap.of(CatalogProperties.URI, url));
    OpenHouseCatalog engineConfigured = new ColumnDefaultsEngineCatalog();
    engineConfigured.initialize(
        "engineConfigured",
        ImmutableMap.of(CatalogProperties.URI, url, "header." + header, " V3,,branching"));
    Map<OpenHouseCatalog, String> expected = new LinkedHashMap<>();
    expected.put(plain, null);
    expected.put(configured, "v3,column-defaults,branching");
    expected.put(engine, "column-defaults");
    expected.put(engineConfigured, "column-defaults,v3,branching");
    for (Map.Entry<OpenHouseCatalog, String> entry : expected.entrySet()) {
      mockTableService.enqueue(new MockResponse().setResponseCode(404));
      Assertions.assertFalse(entry.getKey().tableExists(TableIdentifier.of("db", "table")));
      Assertions.assertEquals(
          entry.getValue(), mockTableService.takeRequest(1, TimeUnit.SECONDS).getHeader(header));
    }
  }

  /** A catalog for an engine that applies column defaults on read, as Spark's does. */
  private static class ColumnDefaultsEngineCatalog extends OpenHouseCatalog {
    @Override
    protected Set<String> capabilities() {
      return Collections.singleton(COLUMN_DEFAULTS_CAPABILITY);
    }
  }

  /**
   * A header property that names no header, has a name or value HTTP cannot carry, names a header
   * the catalog sets, or names the same header as another property is ignored: initialization
   * succeeds and its value is never sent.
   */
  @ParameterizedTest
  @MethodSource("ignoredHeaderProperties")
  public void testUnusableHeaderPropertiesAreIgnored(Map<String, String> headerProperties)
      throws InterruptedException {
    Map<String, String> properties = new HashMap<>(headerProperties);
    properties.put(CatalogProperties.URI, url);
    OpenHouseCatalog catalog = new OpenHouseCatalog();
    catalog.initialize("ignored", properties);
    mockTableService.enqueue(new MockResponse().setResponseCode(404));
    Assertions.assertFalse(catalog.tableExists(TableIdentifier.of("db", "table")));
    RecordedRequest request = mockTableService.takeRequest(1, TimeUnit.SECONDS);
    Assertions.assertNotNull(request);
    Collection<List<String>> sent = request.getHeaders().toMultimap().values();
    Assertions.assertTrue(
        headerProperties.values().stream()
            .allMatch(value -> sent.stream().noneMatch(values -> values.contains(value))));
  }

  static Stream<Map<String, String>> ignoredHeaderProperties() {
    return Stream.of(
        ImmutableMap.of("header.", "unnamed"),
        ImmutableMap.of("header.Bad Header", "bad-name"),
        ImmutableMap.of("header.X-OpenHouse-Example", "first-line\nsecond-line"),
        ImmutableMap.of("header.Authorization", "configured-authorization"),
        ImmutableMap.of("header.user-agent", "configured-user-agent"),
        ImmutableMap.of("header.X-Client-Name", "configured-client-name"),
        ImmutableMap.of("header.X-OpenHouse-Action-Type", "configured-action-type"),
        ImmutableMap.of("header.session-id", "configured-session-id"),
        ImmutableMap.of("header.Accept", "text/plain"),
        ImmutableMap.of("header.content-type", "application/xml"),
        ImmutableMap.of(
            "header.X-OpenHouse-Example", "first", "header.x-openhouse-example", "second"));
  }

  @Test
  public void testDatabaseApiInterfacesWorking() {
    mockTableService.enqueue(
        new MockResponse()
            .setResponseCode(200)
            .setBody("{\"results\":[]}")
            .addHeader("Content-Type", "application/json"));
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(url);
    DatabaseApi databaseApi = new DatabaseApi(apiClient);
    Assertions.assertTrue(() -> databaseApi.getAllDatabasesV1().block().getResults().isEmpty());
  }

  @Test
  public void testSnapshotApiInterfaceWorking() {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(200).addHeader("Content-Type", "application/json"));
    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(url);
    SnapshotApi snapshotApi = new SnapshotApi(apiClient);
    Assertions.assertDoesNotThrow(
        () ->
            snapshotApi
                .putSnapshotsV1("database", "table", new IcebergSnapshotsRequestBody())
                .block());
  }

  @Test
  public void testCatalogInitializeWorks() {
    OpenHouseCatalog openHouseCatalog = new OpenHouseCatalog();
    openHouseCatalog.initialize("openhouse", ImmutableMap.of(CatalogProperties.URI, url));
  }

  @Test
  public void testMetricsReporterIsLoaded() {
    MetricsReporter reporter =
        CatalogUtil.loadMetricsReporter(
            ImmutableMap.of(
                CatalogProperties.METRICS_REPORTER_IMPL,
                "com.linkedin.openhouse.javaclient.OpenHouseMetricsReporter"));
    assert (reporter instanceof OpenHouseMetricsReporter);
  }

  @Test
  public void testCatalogGetTableWorks() {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(404).addHeader("Content-Type", "application/json"));

    OpenHouseCatalog openHouseCatalog = new OpenHouseCatalog();
    openHouseCatalog.initialize("openhouse", ImmutableMap.of(CatalogProperties.URI, url));
    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> openHouseCatalog.loadTable(TableIdentifier.of("db", "table")));
  }

  @Test
  public void testCatalogMissingMetadataTableDoesNotRetryWithCollapsedBaseIdentifier()
      throws InterruptedException {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(404).addHeader("Content-Type", "application/json"));
    mockTableService.enqueue(
        new MockResponse().setResponseCode(404).addHeader("Content-Type", "application/json"));

    OpenHouseCatalog openHouseCatalog = new OpenHouseCatalog();
    openHouseCatalog.initialize("openhouse", ImmutableMap.of(CatalogProperties.URI, url));

    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> openHouseCatalog.loadTable(TableIdentifier.of("db", "partitions")));

    Assertions.assertEquals(1, mockTableService.getRequestCount());
    RecordedRequest request = mockTableService.takeRequest(1, TimeUnit.SECONDS);
    Assertions.assertNotNull(request);
    Assertions.assertEquals("/v1/databases/db/tables/partitions", request.getPath());
    Assertions.assertNull(mockTableService.takeRequest(100, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testCatalogRefreshTableWorks() {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(404).addHeader("Content-Type", "application/json"));
    OpenHouseCatalog openHouseCatalog = new OpenHouseCatalog();
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, url);
    properties.put("auth-token", "token");
    openHouseCatalog.initialize("openhouse", properties);
    String initialAuthToken = openHouseCatalog.properties().get("auth-token");
    openHouseCatalog.updateAuthToken("newToken");
    String updatedAuthToken = openHouseCatalog.properties().get("auth-token");
    Assertions.assertNotEquals(initialAuthToken, updatedAuthToken);
    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> openHouseCatalog.loadTable(TableIdentifier.of("db", "table")));
  }

  @AfterAll
  static void tearDown() throws IOException {
    mockTableService.shutdown();
  }
}

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
import java.util.HashMap;
import java.util.Map;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
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

/**
 * This class tests out packaging done in :integrations:java:openhouse-java-runtime. The goal of
 * this test is to ensure all of `tableclient` functionality, such as configuring {@link ApiClient},
 * {@link TableApi}, making REST call can be satisfied by the singular jar
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
  public void testCatalogRefreshTableWorks() {
    mockTableService.enqueue(
        new MockResponse().setResponseCode(404).addHeader("Content-Type", "application/json"));
    OpenHouseCatalog openHouseCatalog = new OpenHouseCatalog();
    Map<String, String> properties = new HashMap<>();
    properties.put(CatalogProperties.URI, url);
    properties.put("auth-token", "token");
    openHouseCatalog.initialize("openhouse", properties);
    String initial_auth_token = openHouseCatalog.properties().get("auth-token");
    openHouseCatalog.updateAuthToken("newToken");
    String updated_auth_token = openHouseCatalog.properties().get("auth-token");
    Assertions.assertNotEquals(initial_auth_token, updated_auth_token);
    Assertions.assertThrows(
        NoSuchTableException.class,
        () -> openHouseCatalog.loadTable(TableIdentifier.of("db", "table")));
  }

  @AfterAll
  static void tearDown() throws IOException {
    mockTableService.shutdown();
  }
}

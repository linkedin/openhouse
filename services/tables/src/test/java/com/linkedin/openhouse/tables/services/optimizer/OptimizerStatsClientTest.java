package com.linkedin.openhouse.tables.services.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.common.metrics.MetricsConstant;
import com.linkedin.openhouse.tables.model.TableDto;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

class OptimizerStatsClientTest {

  private static final String TABLE_UUID = "uuid-1";
  private static final String DB = "db1";
  private static final String TABLE = "tbl1";
  private static final Duration BLOCK_MAX = Duration.ofSeconds(5);

  private MockWebServer server;
  private MeterRegistry meterRegistry;
  private OptimizerStatsProperties properties;
  private OptimizerStatsClient client;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    meterRegistry = new SimpleMeterRegistry();
    properties = new OptimizerStatsProperties();
    properties.setEnabled(true);
    properties.setBaseUri(server.url("/").toString());
    properties.setPerAttemptTimeoutMs(1000L);
    properties.setTotalTimeoutMs(2000L);
    properties.setMaxAttempts(3);
    client = new OptimizerStatsClient(buildWebClient(), meterRegistry, properties);
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  private WebClient buildWebClient() {
    HttpClient httpClient = HttpClient.create();
    return WebClient.builder()
        .baseUrl(properties.getBaseUri())
        .clientConnector(new ReactorClientHttpConnector(httpClient))
        .build();
  }

  @Test
  void report_optedIn_sendsRequestWithPayloadFieldsFromSummary() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200));

    TableDto saved =
        optedInBuilder()
            .tableVersion("v3")
            .tableLocation("/data/tables/db1/tbl1")
            .currentSnapshotSummary(summary())
            .build();

    client.reportAsync(saved, saved.getCurrentSnapshotSummary()).block(BLOCK_MAX);

    RecordedRequest req = server.takeRequest(2, TimeUnit.SECONDS);
    assertThat(req).isNotNull();
    assertThat(req.getMethod()).isEqualTo("PUT");
    assertThat(req.getPath()).isEqualTo("/v1/optimizer/stats/" + TABLE_UUID);

    @SuppressWarnings("unchecked")
    Map<String, Object> body = mapper.readValue(req.getBody().readUtf8(), Map.class);
    assertThat(body).containsEntry("databaseName", DB).containsEntry("tableName", TABLE);
    @SuppressWarnings("unchecked")
    Map<String, Object> stats = (Map<String, Object>) body.get("stats");
    @SuppressWarnings("unchecked")
    Map<String, Object> snapshot = (Map<String, Object>) stats.get("snapshot");
    @SuppressWarnings("unchecked")
    Map<String, Object> delta = (Map<String, Object>) stats.get("delta");
    assertThat(snapshot)
        .containsEntry("tableVersion", "v3")
        .containsEntry("tableLocation", "/data/tables/db1/tbl1")
        .containsEntry("tableSizeBytes", 4096)
        .containsEntry("numCurrentFiles", 12);
    assertThat(delta)
        .containsEntry("numFilesAdded", 5)
        .containsEntry("numFilesDeleted", 2)
        .containsEntry("addedSizeBytes", 2048)
        .containsEntry("deletedSizeBytes", 1024);

    assertThat(
            meterRegistry
                .counter(MetricsConstant.OPTIMIZER_STATS_ATTEMPTS, "outcome", "success")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void report_notOptedIn_skipsCallAndIncrementsSkippedCounter() {
    TableDto saved = builderWithoutOptIn().currentSnapshotSummary(summary()).build();

    client.reportAsync(saved, saved.getCurrentSnapshotSummary()).block(BLOCK_MAX);

    assertThat(server.getRequestCount()).isZero();
    assertThat(
            meterRegistry
                .counter(MetricsConstant.OPTIMIZER_STATS_SKIPPED, "reason", "opt_out")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void report_noSnapshot_skipsCallAndIncrementsSkippedCounter() {
    TableDto saved = optedInBuilder().currentSnapshotSummary(null).build();

    client.reportAsync(saved, null).block(BLOCK_MAX);

    assertThat(server.getRequestCount()).isZero();
    assertThat(
            meterRegistry
                .counter(MetricsConstant.OPTIMIZER_STATS_SKIPPED, "reason", "no_snapshot")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void report_5xxThenSuccess_retriesAndSucceeds() {
    server.enqueue(new MockResponse().setResponseCode(503));
    server.enqueue(new MockResponse().setResponseCode(200));

    TableDto saved = optedInBuilder().currentSnapshotSummary(summary()).build();
    client.reportAsync(saved, saved.getCurrentSnapshotSummary()).block(BLOCK_MAX);

    assertThat(server.getRequestCount()).isEqualTo(2);
    assertThat(
            meterRegistry
                .counter(MetricsConstant.OPTIMIZER_STATS_ATTEMPTS, "outcome", "success")
                .count())
        .isEqualTo(1.0);
    assertThat(
            meterRegistry
                .counter(MetricsConstant.OPTIMIZER_STATS_ATTEMPTS, "outcome", "retryable_failure")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void report_allAttemptsFail_swallowsAndIncrementsFailedFinal() {
    properties.setMaxAttempts(2);
    server.enqueue(new MockResponse().setResponseCode(503));
    server.enqueue(new MockResponse().setResponseCode(503));

    TableDto saved = optedInBuilder().currentSnapshotSummary(summary()).build();
    client.reportAsync(saved, saved.getCurrentSnapshotSummary()).block(BLOCK_MAX);

    assertThat(server.getRequestCount()).isEqualTo(2);
    assertThat(
            meterRegistry
                .counter(MetricsConstant.OPTIMIZER_STATS_FAILED_FINAL, "outcome", "server_error")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void report_4xxNonRetryable_doesNotRetry() {
    server.enqueue(new MockResponse().setResponseCode(400));

    TableDto saved = optedInBuilder().currentSnapshotSummary(summary()).build();
    client.reportAsync(saved, saved.getCurrentSnapshotSummary()).block(BLOCK_MAX);

    assertThat(server.getRequestCount()).isEqualTo(1);
    assertThat(
            meterRegistry
                .counter(MetricsConstant.OPTIMIZER_STATS_FAILED_FINAL, "outcome", "client_error")
                .count())
        .isEqualTo(1.0);
  }

  @Test
  void report_perAttemptTimeoutTrips_recordsTimeout() {
    properties.setPerAttemptTimeoutMs(150L);
    properties.setTotalTimeoutMs(600L);
    properties.setMaxAttempts(1);
    client = new OptimizerStatsClient(buildWebClient(), meterRegistry, properties);
    // Connection opens but the server never sends any response — forces Reactor's per-attempt
    // .timeout(150ms) to fire with TimeoutException.
    server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));

    TableDto saved = optedInBuilder().currentSnapshotSummary(summary()).build();
    client.reportAsync(saved, saved.getCurrentSnapshotSummary()).block(BLOCK_MAX);

    assertThat(
            meterRegistry
                .counter(MetricsConstant.OPTIMIZER_STATS_FAILED_FINAL, "outcome", "timeout")
                .count())
        .isEqualTo(1.0);
  }

  // ---- helpers ----

  private static Map<String, String> summary() {
    Map<String, String> s = new HashMap<>();
    s.put("total-data-files", "12");
    s.put("total-files-size", "4096");
    s.put("added-data-files", "5");
    s.put("deleted-data-files", "2");
    s.put("added-files-size", "2048");
    s.put("removed-files-size", "1024");
    return s;
  }

  private static TableDto.TableDtoBuilder optedInBuilder() {
    Map<String, String> props = new HashMap<>();
    props.put(OptimizerStatsClient.OPT_IN_PROPERTY, "true");
    return TableDto.builder()
        .tableUUID(TABLE_UUID)
        .databaseId(DB)
        .tableId(TABLE)
        .tableProperties(props);
  }

  private static TableDto.TableDtoBuilder builderWithoutOptIn() {
    return TableDto.builder()
        .tableUUID(TABLE_UUID)
        .databaseId(DB)
        .tableId(TABLE)
        .tableProperties(new HashMap<>());
  }
}

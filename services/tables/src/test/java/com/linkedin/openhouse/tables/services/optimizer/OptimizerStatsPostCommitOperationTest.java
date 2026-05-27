package com.linkedin.openhouse.tables.services.optimizer;

import static org.assertj.core.api.Assertions.assertThat;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.linkedin.openhouse.tables.model.CurrentSnapshotInfo;
import com.linkedin.openhouse.tables.model.TableDto;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

class OptimizerStatsPostCommitOperationTest {

  private static final String TABLE_UUID = "uuid-1";
  private static final String DB = "db1";
  private static final String TABLE = "tbl1";
  private static final long SNAPSHOT_ID = 9876543210L;
  private static final Duration BLOCK_MAX = Duration.ofSeconds(5);

  private MockWebServer server;
  private OptimizerStatsProperties properties;
  private OptimizerStatsPostCommitOperation op;
  private final ObjectMapper mapper = new ObjectMapper();

  @BeforeEach
  void setUp() throws IOException {
    server = new MockWebServer();
    server.start();
    properties = new OptimizerStatsProperties();
    properties.setEnabled(true);
    properties.setBaseUri(server.url("/").toString());
    properties.setPerAttemptTimeoutMs(1000L);
    properties.setMaxAttempts(3);
    op = new OptimizerStatsPostCommitOperation(buildWebClient(), properties);
  }

  @AfterEach
  void tearDown() throws IOException {
    server.shutdown();
  }

  private WebClient buildWebClient() {
    return WebClient.builder()
        .baseUrl(properties.getBaseUri())
        .clientConnector(new ReactorClientHttpConnector(HttpClient.create()))
        .build();
  }

  @Test
  void name_isStableTagValue() {
    assertThat(op.name()).isEqualTo("optimizer_stats");
  }

  @Test
  void prepare_optedIn_emitsRequestWithFullPayload() throws Exception {
    server.enqueue(new MockResponse().setResponseCode(200));

    TableDto saved =
        optedInBuilder()
            .tableVersion("v3")
            .tableLocation("/data/tables/db1/tbl1")
            .currentSnapshot(snapshot())
            .build();

    Optional<Mono<Void>> work = op.prepare(saved);
    assertThat(work).isPresent();
    work.get().block(BLOCK_MAX);

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
        .containsEntry("snapshotId", SNAPSHOT_ID)
        .containsEntry("tableVersion", "v3")
        .containsEntry("tableLocation", "/data/tables/db1/tbl1")
        .containsEntry("tableSizeBytes", 4096)
        .containsEntry("numCurrentFiles", 12);
    assertThat(delta)
        .containsEntry("numFilesAdded", 5)
        .containsEntry("numFilesDeleted", 2)
        .containsEntry("addedSizeBytes", 2048)
        .containsEntry("deletedSizeBytes", 1024);
  }

  @Test
  void prepare_notOptedIn_returnsEmpty() {
    TableDto saved = builderWithoutOptIn().currentSnapshot(snapshot()).build();
    assertThat(op.prepare(saved)).isEmpty();
    assertThat(server.getRequestCount()).isZero();
  }

  @Test
  void prepare_noSnapshot_returnsEmpty() {
    TableDto saved = optedInBuilder().currentSnapshot(null).build();
    assertThat(op.prepare(saved)).isEmpty();
    assertThat(server.getRequestCount()).isZero();
  }

  @Test
  void prepare_5xxThenSuccess_retriesAndCompletes() {
    server.enqueue(new MockResponse().setResponseCode(503));
    server.enqueue(new MockResponse().setResponseCode(200));

    TableDto saved = optedInBuilder().currentSnapshot(snapshot()).build();
    op.prepare(saved).orElseThrow(AssertionError::new).block(BLOCK_MAX);

    assertThat(server.getRequestCount()).isEqualTo(2);
  }

  @Test
  void prepare_allAttemptsFail_propagatesUnderlyingError() {
    properties.setMaxAttempts(2);
    server.enqueue(new MockResponse().setResponseCode(503));
    server.enqueue(new MockResponse().setResponseCode(503));

    TableDto saved = optedInBuilder().currentSnapshot(snapshot()).build();
    Mono<Void> chain = op.prepare(saved).orElseThrow(AssertionError::new);

    assertThatChainErrors(chain, WebClientResponseException.class);
    assertThat(server.getRequestCount()).isEqualTo(2);
  }

  @Test
  void prepare_4xxNonRetryable_doesNotRetry() {
    server.enqueue(new MockResponse().setResponseCode(400));

    TableDto saved = optedInBuilder().currentSnapshot(snapshot()).build();
    Mono<Void> chain = op.prepare(saved).orElseThrow(AssertionError::new);

    assertThatChainErrors(chain, WebClientResponseException.class);
    assertThat(server.getRequestCount()).isEqualTo(1);
  }

  // ---- helpers ----

  private void assertThatChainErrors(Mono<Void> chain, Class<? extends Throwable> errorType) {
    try {
      chain.block(BLOCK_MAX);
      throw new AssertionError("expected chain to signal an error of type " + errorType);
    } catch (RuntimeException e) {
      Throwable cause = unwrap(e);
      assertThat(cause).isInstanceOf(errorType);
    }
  }

  private static Throwable unwrap(Throwable t) {
    Throwable cur = t;
    while (cur.getCause() != null && cur.getCause() != cur) {
      cur = cur.getCause();
    }
    return cur;
  }

  private static CurrentSnapshotInfo snapshot() {
    return CurrentSnapshotInfo.builder().snapshotId(SNAPSHOT_ID).summary(summary()).build();
  }

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
    props.put(OptimizerStatsPostCommitOperation.OPT_IN_PROPERTY, "true");
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

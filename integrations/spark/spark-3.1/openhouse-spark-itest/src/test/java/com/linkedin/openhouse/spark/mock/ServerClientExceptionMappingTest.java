package com.linkedin.openhouse.spark.mock;

import com.linkedin.openhouse.gen.tables.client.api.SnapshotApi;
import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.javaclient.OpenHouseTableOperations;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.javaclient.exception.WebClientWithMessageException;
import java.util.UUID;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Comprehensive test documenting the server-to-client exception mapping for both refresh (read) and
 * commit (write) paths.
 *
 * <p>This test verifies what exception the catalog client ({@code OpenHouseTableOperations}) sees
 * for each HTTP status code returned by the tables-service. This mapping is critical because the
 * client-side exception type determines Iceberg's behavior:
 *
 * <ul>
 *   <li>{@code o.a.iceberg.exceptions.CommitStateUnknownException}: Iceberg does NOT clean up
 *       locally written metadata files — commit may have succeeded on server
 *   <li>{@code o.a.iceberg.exceptions.CommitFailedException}: Iceberg retries the commit (refreshes
 *       metadata and recomputes). After retries are exhausted, cleans up uncommitted files. See
 *       {@code SnapshotProducer.java:380} for retry and {@code :413} for cleanup.
 *   <li>{@code o.a.iceberg.exceptions.BadRequestException}: Iceberg cleans up uncommitted files
 *       (known failure, no retry)
 *   <li>{@code o.a.iceberg.exceptions.NoSuchTableException}: Iceberg cleans up uncommitted files
 *       (known failure)
 *   <li>{@code c.l.openhouse.javaclient.exception.WebClientResponseWithMessageException}: OpenHouse
 *       exception, not known to Iceberg — treated as generic RuntimeException, cleans up
 *       uncommitted files
 * </ul>
 *
 * <p>Server-side exception → HTTP status mapping is defined in {@code OpenHouseExceptionHandler}
 * and tested in {@code TablesControllerTest.testCreateUpdateResponseCodeForVariousExceptions()}.
 * Client-side HTTP status → exception mapping is defined in:
 *
 * <ul>
 *   <li>{@code OpenHouseTableOperations.doRefresh()} for read path
 *   <li>{@code OpenHouseTableOperations.handleCreateUpdateHttpError()} for write path
 * </ul>
 *
 * <p>Iceberg write behavior (cleanup/retry) is determined by exception type in {@code
 * SnapshotProducer.java:411-418}: {@code CommitStateUnknownException} is re-thrown without cleanup,
 * {@code CommitFailedException} is retried via {@code Tasks.onlyRetryOn()}, and all other {@code
 * RuntimeException}s trigger {@code cleanAll()} before re-throwing.
 *
 * <pre>
 * Server Exception                         → HTTP → Client (refresh/read)                                               → Client (commit/write) → Iceberg Write Behavior
 * ──────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────
 * NoSuchUserTableException                 → 404  → o.a.iceberg.exceptions.NoSuchTableException                        → o.a.iceberg.exceptions.NoSuchTableException → cleans up uncommitted files
 * RequestValidationFailureException        → 400  → o.a.iceberg.exceptions.NoSuchTableException                        → o.a.iceberg.exceptions.BadRequestException → cleans up uncommitted files
 * IllegalArgumentException                 → 400  → o.a.iceberg.exceptions.NoSuchTableException                        → o.a.iceberg.exceptions.BadRequestException → cleans up uncommitted files
 * InvalidTableMetadataException            → 500  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → o.a.iceberg.exceptions.CommitStateUnknownException → no cleanup
 * IllegalStateException                    → 500  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → o.a.iceberg.exceptions.CommitStateUnknownException → no cleanup
 * Exception (generic)                      → 500  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → o.a.iceberg.exceptions.CommitStateUnknownException → no cleanup
 * EntityConcurrentModificationException    → 409  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → o.a.iceberg.exceptions.CommitFailedException → retries commit, cleans up if retries exhausted
 * AlreadyExistsException                   → 409  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → o.a.iceberg.exceptions.CommitFailedException → retries commit, cleans up if retries exhausted
 * OpenHouseCommitStateUnknownException     → 503  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → o.a.iceberg.exceptions.CommitStateUnknownException → no cleanup
 * AuthorizationServiceException            → 503  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → o.a.iceberg.exceptions.CommitStateUnknownException → no cleanup
 * (gateway timeout)                        → 504  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → o.a.iceberg.exceptions.CommitStateUnknownException → no cleanup
 * AccessDeniedException                    → 403  → c.l.openhouse.javaclient.exception.WebClientResponseWithMessageExc  → c.l.openhouse.javaclient.exception.WebClientResponseWithMsgExc → cleans up uncommitted files
 * </pre>
 */
public class ServerClientExceptionMappingTest {

  private MockWebServer server;
  private OpenHouseTableOperations ops;
  private TableMetadata base;

  @BeforeEach
  public void setup() throws Exception {
    server = new MockWebServer();
    server.start();

    ApiClient apiClient = new ApiClient();
    apiClient.setBasePath(String.format("http://%s:%s", server.getHostName(), server.getPort()));

    ops =
        OpenHouseTableOperations.builder()
            .tableApi(new TableApi(apiClient))
            .snapshotApi(new SnapshotApi(apiClient))
            .fileIO(new HadoopFileIO(new Configuration()))
            .tableIdentifier(TableIdentifier.of("db", "tbl"))
            .build();
    base =
        TableMetadata.newTableMetadata(
            new Schema(
                Types.NestedField.required(1, "col1", Types.StringType.get()),
                Types.NestedField.required(2, "col2", Types.TimestampType.withoutZone())),
            PartitionSpec.unpartitioned(),
            UUID.randomUUID().toString(),
            ImmutableMap.of());
  }

  @AfterEach
  public void teardown() throws Exception {
    server.shutdown();
  }

  private MockResponse jsonResponse(int status, String message) {
    return new MockResponse()
        .setResponseCode(status)
        .setBody("{\"message\":\"" + message + "\"}")
        .addHeader("Content-Type", "application/json");
  }

  // ==================== REFRESH (READ) PATH ====================

  @Test
  public void testRefresh_404_swallowed() {
    server.enqueue(jsonResponse(404, "Not Found"));
    Assertions.assertDoesNotThrow(() -> ops.doRefresh());
  }

  @Test
  public void testRefresh_400_swallowed() {
    server.enqueue(jsonResponse(400, "Bad Request"));
    Assertions.assertDoesNotThrow(() -> ops.doRefresh());
  }

  @Test
  public void testRefresh_409_surfaced() {
    server.enqueue(jsonResponse(409, "Conflict"));
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }

  @Test
  public void testRefresh_403_surfaced() {
    server.enqueue(jsonResponse(403, "Forbidden"));
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }

  @Test
  public void testRefresh_500_surfaced() {
    server.enqueue(jsonResponse(500, "Internal Server Error"));
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }

  @Test
  public void testRefresh_503_surfaced() {
    server.enqueue(jsonResponse(503, "Service Unavailable"));
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }

  @Test
  public void testRefresh_504_surfaced() {
    server.enqueue(jsonResponse(504, "Gateway Timeout"));
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }

  // ==================== COMMIT (WRITE) PATH ====================

  @Test
  public void testCommit_404_noSuchTable() {
    server.enqueue(jsonResponse(404, "Not Found"));
    Assertions.assertThrows(NoSuchTableException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testCommit_400_badRequest() {
    server.enqueue(jsonResponse(400, "Bad Request"));
    Assertions.assertThrows(BadRequestException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testCommit_409_commitFailed() {
    server.enqueue(jsonResponse(409, "Concurrent Update"));
    Assertions.assertThrows(CommitFailedException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testCommit_500_commitStateUnknown() {
    server.enqueue(jsonResponse(500, "Internal Server Error"));
    Assertions.assertThrows(CommitStateUnknownException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testCommit_503_commitStateUnknown() {
    server.enqueue(jsonResponse(503, "Service Unavailable"));
    Assertions.assertThrows(CommitStateUnknownException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testCommit_504_commitStateUnknown() {
    server.enqueue(jsonResponse(504, "Gateway Timeout"));
    Assertions.assertThrows(CommitStateUnknownException.class, () -> ops.doCommit(null, base));
  }

  @Test
  public void testCommit_403_surfaced() {
    server.enqueue(jsonResponse(403, "Forbidden"));
    Assertions.assertThrows(
        WebClientResponseWithMessageException.class, () -> ops.doCommit(null, base));
  }
}

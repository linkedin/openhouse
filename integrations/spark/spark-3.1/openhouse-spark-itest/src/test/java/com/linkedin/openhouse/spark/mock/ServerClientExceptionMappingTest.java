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
 * <p>This test verifies what Iceberg exception the catalog client sees for each HTTP status code
 * returned by the tables-service. This mapping is critical because it determines whether Iceberg
 * will clean up committed metadata files on the client side:
 *
 * <ul>
 *   <li>{@link CommitStateUnknownException}: Iceberg does NOT clean up (safe)
 *   <li>{@link CommitFailedException}: Iceberg cleans up (retryable conflict)
 *   <li>{@link BadRequestException}: Iceberg cleans up (known failure)
 *   <li>{@link NoSuchTableException}: Iceberg cleans up (known failure)
 * </ul>
 *
 * <p>Server-side exception → HTTP status mapping is defined in {@code OpenHouseExceptionHandler}.
 * Client-side HTTP status → Iceberg exception mapping is defined in:
 *
 * <ul>
 *   <li>{@code OpenHouseTableOperations.doRefresh()} for read path
 *   <li>{@code OpenHouseTableOperations.handleCreateUpdateHttpError()} for write path
 * </ul>
 *
 * <pre>
 * Server Exception                    → HTTP  → Client (refresh/read)              → Client (commit/write)
 * ─────────────────────────────────────────────────────────────────────────────────────────────────────────
 * NoSuchUserTableException            → 404   → swallowed (no error)               → NoSuchTableException
 * RequestValidationFailureException   → 400   → swallowed (no error)               → BadRequestException
 * InvalidTableMetadataException       → 500   → WebClientResponseWithMessageExc    → CommitStateUnknownException
 * IllegalStateException               → 500   → WebClientResponseWithMessageExc    → CommitStateUnknownException
 * EntityConcurrentModification        → 409   → WebClientResponseWithMessageExc    → CommitFailedException
 * OpenHouseCommitStateUnknown         → 503   → WebClientResponseWithMessageExc    → CommitStateUnknownException
 * AlreadyExistsException              → 409   → WebClientResponseWithMessageExc    → CommitFailedException
 * AccessDeniedException               → 403   → WebClientResponseWithMessageExc    → WebClientResponseWithMessageExc
 * IllegalArgumentException            → 400   → swallowed (no error)               → BadRequestException
 * AuthorizationServiceException       → 503   → WebClientResponseWithMessageExc    → CommitStateUnknownException
 * Exception (generic)                 → 500   → WebClientResponseWithMessageExc    → CommitStateUnknownException
 * GatewayTimeout                      → 504   → WebClientResponseWithMessageExc    → CommitStateUnknownException
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

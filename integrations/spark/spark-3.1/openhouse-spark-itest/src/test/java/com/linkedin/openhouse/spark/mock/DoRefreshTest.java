package com.linkedin.openhouse.spark.mock;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.javaclient.OpenHouseTableOperations;
import com.linkedin.openhouse.javaclient.exception.WebClientWithMessageException;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableList;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class DoRefreshTest {

  private OpenHouseTableOperations ops;

  @BeforeEach
  public void setup() {
    ops =
        OpenHouseTableOperations.builder()
            .tableApi(getTableApiClient())
            .fileIO(new HadoopFileIO(new Configuration()))
            .tableIdentifier(TableIdentifier.of("db", "tbl"))
            .build();
  }

  @Test
  public void testDontSurfaceErrorOn404() {
    mockTableService.enqueue(mockResponse(404, mockGetAllTableResponseBody()));
    Assertions.assertDoesNotThrow(() -> ops.doRefresh());
  }

  @Test
  public void testDontSurfaceErrorOnNullLocation() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody("db", "tbl", "", "", "", null, "", baseSchema, null, null)));
    Assertions.assertDoesNotThrow(() -> ops.doRefresh());
  }

  @Test
  public void testGoodTableLocation() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetTableResponseBody(
                "db",
                "tbl",
                "",
                "",
                "",
                mockTableLocationDefaultSchema(TableIdentifier.of("db", "tbl")),
                "",
                baseSchema,
                null,
                null)));
    Assertions.assertDoesNotThrow(() -> ops.doRefresh());
    Assertions.assertNotNull(ops.currentMetadataLocation());
  }

  @Test
  public void testSurfaceEveryOtherError() {
    for (int status : ImmutableList.of(408, 500)) {
      mockTableService.enqueue(mockResponse(status, mockGetAllTableResponseBody()));
      Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
    }
  }

  /**
   * Verifies that a 400 (BadRequest) from the server is silently swallowed in doRefresh. This is
   * the current behavior — the catalog client treats 400 the same as 404 (table not found). This
   * test documents the existing behavior; a follow-up change may surface 400 errors instead.
   */
  @Test
  public void testBadRequestSwallowedOnRefresh() {
    mockTableService.enqueue(mockResponse(400, "{\"message\":\"Bad Request\"}"));
    Assertions.assertDoesNotThrow(() -> ops.doRefresh());
  }

  /**
   * Verifies that server-side errors (500) surface as WebClientWithMessageException on the client
   * side during doRefresh. This is the expected behavior for InvalidTableMetadataException (corrupt
   * metadata) which maps to 500 on the server. The client should NOT swallow this error — it must
   * propagate so users see the actual error message instead of "Table does not exist".
   */
  @Test
  public void testServerErrorSurfacedOnRefresh() {
    mockTableService.enqueue(
        mockResponse(
            500,
            "{\"message\":\"Table db.tbl has invalid metadata: Cannot find schema with current-schema-id=6\"}"));
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }

  @Test
  public void testConnectionRefusedError() throws IOException {
    mockTableService.shutdown();
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }
}

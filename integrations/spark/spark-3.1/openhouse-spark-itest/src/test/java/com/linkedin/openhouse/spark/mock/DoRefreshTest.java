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
   * Verifies that server-side 5xx errors surface as WebClientWithMessageException on the client
   * side during doRefresh (the client must NOT swallow them — users must see the real error instead
   * of "Table does not exist"). Post-BDP-108628, corrupt metadata maps to 422 and transient storage
   * to 503; a 500 now represents an unexpected OpenHouse implementation defect.
   */
  @Test
  public void testServerErrorSurfacedOnRefresh() {
    mockTableService.enqueue(
        mockResponse(
            500,
            "{\"message\":\"Table db.tbl unexpected error loading metadata (possible OpenHouse defect)\"}"));
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }

  @Test
  public void testConnectionRefusedError() throws IOException {
    mockTableService.shutdown();
    Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
  }
}

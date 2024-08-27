package com.linkedin.openhouse.spark.mock;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.javaclient.OpenHouseTableOperations;
import com.linkedin.openhouse.javaclient.exception.WebClientWithMessageException;
import com.linkedin.openhouse.relocated.org.springframework.http.HttpStatus;
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

  @Test
  public void testConnectionRefusedError() throws IOException {
    mockTableService.shutdown();
    WebClientWithMessageException exception =
        Assertions.assertThrows(WebClientWithMessageException.class, () -> ops.doRefresh());
    Assertions.assertEquals(exception.getStatusCode(), HttpStatus.BAD_REQUEST.value());
  }
}

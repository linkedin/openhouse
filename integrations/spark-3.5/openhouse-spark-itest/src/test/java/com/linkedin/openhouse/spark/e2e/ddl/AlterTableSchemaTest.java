package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.linkedin.openhouse.spark.ResourceIoHelper;
import com.linkedin.openhouse.spark.SparkTestBase;
import lombok.SneakyThrows;
import org.apache.iceberg.catalog.TableIdentifier;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class AlterTableSchemaTest {

  @SneakyThrows
  @Test
  public void testAlterTableUpdateSchema() {
    Object existingTable =
        mockGetTableResponseBody(
            "dbAlterS",
            "tb1",
            "c1",
            "dbAlterS.tb1.c1",
            "UUID",
            mockTableLocationDefaultSchema(TableIdentifier.of("dbAlterS", "tb1")),
            "v1",
            baseSchema,
            null,
            null);
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue(mockResponse(200, existingTable)); // doRefresh()
    mockTableService.enqueue( // doCommit()
        mockResponse(
            200,
            mockGetTableResponseBody(
                "dbAlterS",
                "tb1",
                "c1",
                "dbAlterS.tb1.c1",
                "UUID",
                "file:/loc2",
                "v1",
                ResourceIoHelper.getSchemaJsonFromResource("evolved_dummy_healthy_schema.json"),
                null,
                null)));

    Assertions.assertDoesNotThrow(
        () -> spark.sql("ALTER TABLE openhouse.dbAlterS.tb1 ADD columns (favorite_number int)"));
  }
}

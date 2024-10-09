package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iceberg.exceptions.ValidationException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class ShowTablesTest {

  @Test
  public void testShowTables() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetAllTableResponseBody(
                mockGetTableResponseBody(
                    "dbShow", "tb1", null, null, null, null, null, null, null, null),
                mockGetTableResponseBody(
                    "dbShow", "tb2", null, null, null, null, null, null, null, null))));

    List<String> actualRows =
        spark.sql("SHOW TABLES IN openhouse.dbShow").collectAsList().stream()
            .map(row -> row.mkString("."))
            .collect(Collectors.toList());

    Assertions.assertTrue(actualRows.containsAll(ImmutableList.of("dbShow.tb1", "dbShow.tb2")));
  }

  @Test
  public void testShowTablesEmpty() {
    mockTableService.enqueue(mockResponse(200, mockGetAllTableResponseBody()));

    Assertions.assertTrue(spark.sql("SHOW TABLES IN openhouse.dbShow").collectAsList().isEmpty());
  }

  @Test
  public void testShowTablesValidationFailure() {
    ValidationException validationException =
        Assertions.assertThrows(
            ValidationException.class, () -> spark.sql("SHOW TABLES in openhouse"));
    Assertions.assertTrue(validationException.getMessage().contains("DatabaseId was not provided"));

    spark.sql("Use openhouse");
    validationException =
        Assertions.assertThrows(ValidationException.class, () -> spark.sql("SHOW TABLES"));
    Assertions.assertTrue(validationException.getMessage().contains("DatabaseId was not provided"));
  }
}

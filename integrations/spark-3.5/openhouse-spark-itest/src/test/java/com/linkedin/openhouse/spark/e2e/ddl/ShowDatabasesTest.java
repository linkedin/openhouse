package com.linkedin.openhouse.spark.e2e.ddl;

import static com.linkedin.openhouse.spark.MockHelpers.*;
import static com.linkedin.openhouse.spark.SparkTestBase.*;

import com.google.common.collect.ImmutableList;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(SparkTestBase.class)
public class ShowDatabasesTest {
  @Test
  public void testShowDatabases() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetAllDatabasesResponseBody(
                mockGetDatabaseResponseBody("dbShowDb", "tb1"),
                mockGetDatabaseResponseBody("dbShowDb2", "tb1"))));

    spark.sql("USE openhouse");
    List<String> actualRows =
        spark.sql("SHOW DATABASES").collectAsList().stream()
            .map(row -> row.mkString())
            .collect(Collectors.toList());

    Assertions.assertTrue(actualRows.containsAll(ImmutableList.of("dbShowDb", "dbShowDb2")));
  }

  @Test
  public void testShowDatabasesLike() {
    mockTableService.enqueue(
        mockResponse(
            200,
            mockGetAllDatabasesResponseBody(
                mockGetDatabaseResponseBody("dbShowDb", "tb1"),
                mockGetDatabaseResponseBody("dbShowDb2", "tb1"))));

    spark.sql("USE openhouse");
    List<String> actualRows =
        spark.sql("SHOW DATABASES LIKE 'db*'").collectAsList().stream()
            .map(row -> row.mkString())
            .collect(Collectors.toList());

    Assertions.assertTrue(actualRows.containsAll(ImmutableList.of("dbShowDb", "dbShowDb2")));
  }

  @Test
  public void testShowDatabasesEmpty() {
    mockTableService.enqueue(mockResponse(200, mockGetAllTableResponseBody()));

    spark.sql("USE openhouse");
    Assertions.assertTrue(spark.sql("SHOW DATABASES").collectAsList().isEmpty());
  }
}

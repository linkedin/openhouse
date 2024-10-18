package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

public class PartitionTest extends OpenHouseSparkITest {
  @Test
  public void testCreateTablePartitionedWithNestedColumn() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      List<String> transformList =
          Arrays.asList("days(time)", "header.time", "truncate(10, header.time)");
      List<String> expectedResult =
          Arrays.asList("days(time)", "bigint", "truncate(10, header.time)");
      for (int i = 0; i < transformList.size(); i++) {
        String transform = transformList.get(i);
        String tableName =
            transform
                .replaceAll("\\.", "_")
                .replaceAll("\\(", "_")
                .replaceAll("\\)", "")
                .replaceAll(", ", "_");
        spark.sql(
            String.format(
                "CREATE TABLE openhouse.d1.%s (time timestamp, header struct<time:long, name:string>) partitioned by (%s)",
                tableName, transform));
        // verify that partition spec is correct
        List<String> description =
            spark.sql(String.format("DESCRIBE TABLE openhouse.d1.%s", tableName))
                .select("data_type").collectAsList().stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());
        assertTrue(description.contains(expectedResult.get(i)));
        spark.sql(String.format("DROP TABLE openhouse.d1.%s", tableName));
      }
    }
  }
}

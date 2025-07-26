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
          Arrays.asList("days(time)", "header.time", "truncate(header.time, 10)");
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

  @Test
  public void testCreateTablePartitionedWithBucketTransform() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      List<String> transformList =
          Arrays.asList("bucket(2, name)", "bucket(4, id)", "bucket(8, category)");
      List<String> expectedResult =
          Arrays.asList("bucket(2, name)", "bucket(4, id)", "bucket(8, category)");
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
                "CREATE TABLE openhouse.d1.%s (id string, name string, category string, timestamp timestamp) partitioned by (%s)",
                tableName, transform));

        // Insert some test data to verify bucketing works
        spark.sql(
            String.format(
                "INSERT INTO openhouse.d1.%s VALUES ('1', 'alice', 'A', current_timestamp())",
                tableName));
        spark.sql(
            String.format(
                "INSERT INTO openhouse.d1.%s VALUES ('2', 'bob', 'B', current_timestamp())",
                tableName));

        // Verify that partition spec is correct
        List<String> description =
            spark.sql(String.format("DESCRIBE TABLE openhouse.d1.%s", tableName))
                .select("data_type").collectAsList().stream()
                .map(row -> row.getString(0))
                .collect(Collectors.toList());
        assertTrue(description.contains(expectedResult.get(i)));

        // Verify data was inserted successfully
        assertEquals(
            2, spark.sql(String.format("SELECT * FROM openhouse.d1.%s", tableName)).count());

        spark.sql(String.format("DROP TABLE openhouse.d1.%s", tableName));
      }
    }
  }

  @Test
  public void testBucketPartitioningCreatesCorrectNumberOfPartitions() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      // Test different bucket sizes
      int[] bucketSizes = {2, 4, 8};
      int numValuesToInsert = 20; // Insert more values than bucket count

      for (int bucketCount : bucketSizes) {
        String tableName = String.format("bucket_partition_test_%d", bucketCount);
        String transform = String.format("bucket(%d, id)", bucketCount);

        // Create table with bucket partitioning
        spark.sql(
            String.format(
                "CREATE TABLE openhouse.d1.%s (id int, name string, value double) partitioned by (%s)",
                tableName, transform));

        // Insert values from 0 to N
        for (int i = 0; i < numValuesToInsert; i++) {
          spark.sql(
              String.format(
                  "INSERT INTO openhouse.d1.%s VALUES (%d, 'name_%d', %f)",
                  tableName, i, i, i * 1.5));
        }

        // Verify all data was inserted
        assertEquals(
            numValuesToInsert,
            spark.sql(String.format("SELECT * FROM openhouse.d1.%s", tableName)).count());

        // Check that we have exactly bucketCount partitions
        long partitionCount =
            spark.sql(String.format("SELECT * FROM openhouse.d1.%s.partitions", tableName)).count();
        assertEquals(
            bucketCount,
            partitionCount,
            String.format(
                "Expected %d partitions for bucket(%d, id) but found %d",
                bucketCount, bucketCount, partitionCount));

        // Verify that each partition has some data (since we inserted enough values)
        List<Integer> partitionFileCounts =
            spark.sql(String.format("SELECT file_count FROM openhouse.d1.%s.partitions", tableName))
                .select("file_count").collectAsList().stream()
                .map(row -> row.getInt(0))
                .collect(Collectors.toList());

        // All partitions should have at least one file
        assertTrue(
            partitionFileCounts.stream().allMatch(count -> count > 0),
            String.format(
                "All partitions should have files, but found counts: %s", partitionFileCounts));

        spark.sql(String.format("DROP TABLE openhouse.d1.%s", tableName));
      }
    }
  }
}

package com.linkedin.openhouse.catalog.e2e;

import static org.apache.iceberg.types.Types.NestedField.*;

import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.spark.SparkTableUtil;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class MinimalSparkMoRTest extends OpenHouseSparkITest {
  private static final Schema SCHEMA =
      new Schema(
          required(1, "id", Types.IntegerType.get()), required(2, "data", Types.StringType.get()));

  private static final PartitionSpec SPEC =
      PartitionSpec.builderFor(SCHEMA).bucket("data", 16).build();

  private static final DataFile FILE_A =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-a.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=0")
          .withRecordCount(1)
          .build();
  private static final DataFile FILE_B =
      DataFiles.builder(SPEC)
          .withPath("/path/to/data-b.parquet")
          .withFileSizeInBytes(10)
          .withPartitionPath("data_bucket=1")
          .withRecordCount(1)
          .build();

  @Test
  public void testDataCompactionPartialProgressNonPartitionedTable() throws Exception {
    final String tableName = "db.test_data_compaction";
    final int numInserts = 2;

    BiFunction<Operations, Table, RewriteDataFiles.Result> rewriteFunc =
        (ops, table) ->
            ops.rewriteDataFiles(
                table,
                1024 * 1024, // 1MB
                1024, // 1KB
                1024 * 1024 * 2, // 2MB
                2,
                1,
                true,
                10);

    try (Operations ops = Operations.withCatalog(getSparkSession(), null)) {
      prepareTable(ops, tableName, false);
      populateTable(ops, tableName, numInserts);
      //      ops.spark().sql("ALTER TABLE db.test_data_compaction SET TBLPROPERTIES
      // ('write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read',
      // 'write.merge.mode'='merge-on-read', 'write.delete.distribution-mode'='range');").show();
      // ops.spark().sql("DELETE FROM db.test_data_compaction WHERE data = 'v12'").show();
      ops.spark().sql("select * from db.test_data_compaction").drop("summary").show(80, false);
      ops.spark()
          .sql("select * from db.test_data_compaction.snapshots")
          .drop("summary")
          .show(80, false);
      ops.getTable(tableName)
          .updateProperties()
          .set("write.delete.distribution-mode", "range")
          .commit();
      ops.spark().sql("DELETE FROM db.test_data_compaction WHERE data = 'v6'").show();
      Table table = ops.getTable(tableName);
      // log.info("Loaded table {}, location {}", table.name(), table.location());
      RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
      populateTable(ops, tableName, 3);
      Dataset<Row> metadataTable =
          SparkTableUtil.loadMetadataTable(ops.spark(), table, MetadataTableType.FILES)
              .selectExpr("content", "file_path", "file_size_in_bytes")
              .dropDuplicates(
                  "file_path",
                  "file_size_in_bytes"); // tOdo: can the same file_path have two diff sizes

      // Aggregate counts and sums based on `content` values
      Dataset<Row> contentStats =
          metadataTable
              .groupBy("content")
              .agg(
                  functions.count("content").alias("count"),
                  functions.sum("file_size_in_bytes").alias("total_size"));

      // Collect the result as a map for quick lookup of counts and sums by content value
      Map<Integer, Row> statsMap =
          contentStats.collectAsList().stream()
              .collect(
                  Collectors.toMap(
                      row -> row.getInt(0), // content value (0, 1, or 2)
                      row -> row // Row containing count and total_size
                      ));
      // log.info(
      //          "Added {} data files, rewritten {} data files, rewritten {} bytes",
      //          result.addedDataFilesCount(),
      //          result.rewrittenDataFilesCount(),
      //          result.rewrittenBytesCount());
      Assertions.assertEquals(0, result.addedDataFilesCount());
      Assertions.assertEquals(0, result.rewrittenDataFilesCount());

      populateTable(ops, tableName, 3);
      ops.spark().sql("DELETE FROM db.test_data_compaction WHERE data = 'v6'").show();
      populateTable(ops, tableName, 3);
      RewriteDataFiles.Result result2 = rewriteFunc.apply(ops, table);
      Assertions.assertEquals(0, result2.addedDataFilesCount());
      Assertions.assertEquals(0, result2.rewrittenDataFilesCount());
    }
  }

  private static void prepareTable(Operations ops, String tableName, boolean isPartitioned) {
    ops.spark().sql(String.format("DROP TABLE IF EXISTS %s", tableName)).show();
    if (isPartitioned) {
      ops.spark()
          .sql(
              String.format(
                  "CREATE TABLE %s (data string, ts timestamp) partitioned by (days(ts))",
                  tableName))
          .show();
    } else {
      ops.spark()
          .sql(String.format("CREATE TABLE %s (data string, ts timestamp)", tableName))
          .show();
    }
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
  }

  private static List<Long> getSnapshotIds(Operations ops, String tableName) {
    // log.info("Getting snapshot Ids");
    List<Row> snapshots =
        ops.spark().sql(String.format("SELECT * FROM %s.snapshots", tableName)).collectAsList();
    // snapshots.forEach(s -> log.info(s.toString()));
    return snapshots.stream()
        .map(r -> r.getLong(r.fieldIndex("snapshot_id")))
        .collect(Collectors.toList());
  }

  private static void populateTable(Operations ops, String tableName, int numRows) {
    populateTable(ops, tableName, numRows, 0);
  }

  private static void populateTable(Operations ops, String tableName, int numRows, int dayLag) {
    populateTable(ops, tableName, numRows, dayLag, System.currentTimeMillis() / 1000);
  }

  private static void populateTable(
      Operations ops, String tableName, int numRows, int dayLag, long timestampSeconds) {
    String timestampEntry =
        String.format("date_sub(from_unixtime(%d), %d)", timestampSeconds, dayLag);
    StringBuilder valuesBatch = new StringBuilder();
    for (int row = 0; row < numRows; ++row) {
      valuesBatch.setLength(0); // Clear the batch for each iteration
      for (int i = 0; i < 10; i++) {
        valuesBatch.append(String.format("('v%d', %s)", row + i, timestampEntry));
        if (i < 9) {
          valuesBatch.append(", ");
        }
      }

      ops.spark()
          .sql(String.format("INSERT INTO %s VALUES %s", tableName, valuesBatch.toString()))
          .show();

      // ops.spark().sql(String.format("DELETE FROM db.test_data_compaction WHERE data = 'v%d'",
      // row)).show();
    }
  }
}

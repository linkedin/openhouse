package com.linkedin.openhouse.catalog.e2e;

import static org.assertj.core.api.Assertions.*;

import com.linkedin.openhouse.common.stats.model.IcebergTableStats;
import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.jobs.util.SimpleRecord;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.extern.slf4j.Slf4j;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionKey;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.deletes.EqualityDeleteWriter;
import org.apache.iceberg.encryption.EncryptedOutputFile;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.iceberg.util.ArrayUtil;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class SparkMoRFunctionalTest extends OpenHouseSparkITest {
  static final String tableName = "db.test_data_compaction";

  final BiFunction<Operations, Table, RewriteDataFiles.Result> rewriteFunc =
      (ops, table) ->
          ops.rewriteDataFiles(
              table,
              1024 * 1024, // 1MB
              1024, // 1KB
              1024 * 1024 * 2, // 2MB
              2,
              1,
              true,
              10,
              0);

  private Operations ops;

  @BeforeEach
  public void setUp() throws Exception {
    ops = Operations.withCatalog(getSparkSession(), null);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  protected void initTable() {
    sql(
        "ALTER TABLE %s SET TBLPROPERTIES ('write.delete.mode'='merge-on-read', 'write.update.mode'='merge-on-read', 'write.merge.mode'='merge-on-read', 'write.delete.distribution-mode'='range');",
        tableName);
  }

  protected void createAndInitTable(String schema) {
    sql("CREATE TABLE openhouse.%s (%s) USING iceberg %s", tableName, schema, "");
    initTable();
  }

  @Test
  public void testDeleteFilesCanBeCreated() throws NoSuchTableException {
    createAndInitTable("id int, data string");

    List<SimpleRecord> records =
        Arrays.asList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(1, "b"),
            new SimpleRecord(1, "c"),
            new SimpleRecord(2, "d"),
            new SimpleRecord(2, "e"));
    ops.spark()
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    sql("DELETE FROM %s WHERE id = 1 and data='a'", tableName);
    sql("DELETE FROM %s WHERE id = 2 and data='d'", tableName);
    sql("DELETE FROM %s WHERE id = 1 and data='c'", tableName);

    IcebergTableStats stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumPositionDeleteFiles() + stats.getNumEqualityDeleteFiles()).isEqualTo(3L);

    List<Object[]> expected = Arrays.asList(row(1, "b"), row(2, "e"));
    List<Object[]> actual = sql("SELECT * FROM %s ORDER BY id ASC", tableName);
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  @Test
  public void testCompactionCanRemovePositionDeleteFiles() throws NoSuchTableException {
    createAndInitTable("id int, data string");

    List<SimpleRecord> records =
        Arrays.asList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(1, "b"),
            new SimpleRecord(1, "c"),
            new SimpleRecord(2, "d"),
            new SimpleRecord(2, "e"));
    ops.spark()
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    sql("DELETE FROM %s WHERE id = 1 and data='a'", tableName);
    sql("DELETE FROM %s WHERE id = 2 and data='d'", tableName);
    sql("DELETE FROM %s WHERE id = 1 and data='c'", tableName);

    Table table = ops.getTable(tableName);

    // state of table prior to any compaction
    IcebergTableStats stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumPositionDeleteFiles()).isEqualTo(3L);
    assertThat(stats.getNumCurrentSnapshotPositionDeleteFiles()).isEqualTo(3L);

    // first run of compaction
    RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
    Assertions.assertEquals(1, result.addedDataFilesCount());
    Assertions.assertEquals(1, result.rewrittenDataFilesCount());

    // this asserts the dangling delete problem, since compaction did NOT remove the final delete
    stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumPositionDeleteFiles()).isEqualTo(3L);
    assertThat(stats.getNumCurrentSnapshotPositionDeleteFiles()).isEqualTo(1L);

    // notice that running compaction again with no new commits cleans up the dangling delete
    rewriteFunc.apply(ops, table);
    stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumPositionDeleteFiles()).isEqualTo(3L);
    assertThat(stats.getNumCurrentSnapshotPositionDeleteFiles()).isEqualTo(0L);
  }

  @Test
  public void testCompactionCanRemoveEqualityDeleteFiles() throws NoSuchTableException {
    createAndInitTable("id int, data string");

    List<SimpleRecord> records =
        Arrays.asList(
            new SimpleRecord(1, "a"),
            new SimpleRecord(1, "b"),
            new SimpleRecord(1, "c"),
            new SimpleRecord(2, "d"),
            new SimpleRecord(2, "e"));

    ops.spark()
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    // Create an equality delete manually since cannot force sql to do it
    Table table = ops.getTable(tableName);
    writeEqDeleteRecord(table, "data", "a");
    writeEqDeleteRecord(table, "data", "d");
    writeEqDeleteRecord(table, "data", "c");

    // state of table prior to any compaction
    IcebergTableStats stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumEqualityDeleteFiles()).isEqualTo(3L);
    assertThat(stats.getNumCurrentSnapshotEqualityDeleteFiles()).isEqualTo(3L);

    // first run of compaction
    RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
    Assertions.assertEquals(1, result.addedDataFilesCount());
    Assertions.assertEquals(1, result.rewrittenDataFilesCount());

    // this asserts the dangling delete problem, since compaction did NOT remove the final delete
    stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumEqualityDeleteFiles()).isEqualTo(3L);
    assertThat(stats.getNumCurrentSnapshotEqualityDeleteFiles()).isEqualTo(1L);

    // notice that running compaction again with no new commits cleans up the dangling delete
    rewriteFunc.apply(ops, table);
    stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumEqualityDeleteFiles()).isEqualTo(3L);
    assertThat(stats.getNumCurrentSnapshotEqualityDeleteFiles()).isEqualTo(0L);
  }

  @Test
  public void testBudgetedRewriteUsesDataLengthForTaskGrouping() throws NoSuchTableException {
    createAndInitTable("id int, data string");

    // Create 4 separate data files by appending individually
    for (int i = 0; i < 4; i++) {
      List<SimpleRecord> records =
          Arrays.asList(
              new SimpleRecord(i * 2, "data_" + i), new SimpleRecord(i * 2 + 1, "data_" + i));
      ops.spark()
          .createDataset(records, Encoders.bean(SimpleRecord.class))
          .coalesce(1)
          .writeTo(tableName)
          .append();
    }

    // Delete one row from each data file to produce partition-scoped position delete files.
    // In an unpartitioned table, all position deletes are in the same partition and thus
    // associated with ALL data files, inflating each task's sizeBytes relative to its length.
    for (int i = 0; i < 4; i++) {
      sql("DELETE FROM %s WHERE id = %d", tableName, i * 2);
    }

    Table table = ops.getTable(tableName);

    // Verify we have 4 data files and position delete files
    List<Object[]> dataFileCountResult = sql("SELECT count(*) FROM %s.data_files", tableName);
    assertThat((long) dataFileCountResult.get(0)[0]).isEqualTo(4L);

    List<Object[]> deleteFileCountResult = sql("SELECT count(*) FROM %s.delete_files", tableName);
    assertThat((long) deleteFileCountResult.get(0)[0]).isGreaterThanOrEqualTo(4L);

    // Compute budget as half of total data file size (by file_size_in_bytes from metadata,
    // excluding delete file sizes). If the old sizeBytes-based grouping was used, each task
    // would appear much larger (data + all partition-scoped delete files), and the budget
    // would cover fewer files.
    List<Object[]> totalSizeResult =
        sql("SELECT sum(file_size_in_bytes) FROM %s.data_files", tableName);
    long totalDataSize = (long) totalSizeResult.get(0)[0];
    long halfBudget = totalDataSize / 2;

    // Set target-file-size-bytes to the total size of 2 data files. With the length-based
    // grouping fix (linkedin/iceberg#233), the 2 rewritten data files are grouped into a
    // single task and merged into 1 output file. If sizeBytes (data + all partition-scoped
    // delete files) was used instead, each task would appear much larger than the target,
    // preventing them from being grouped together and producing 2 separate output files.
    long targetSize = halfBudget;

    log.info(
        "Budgeted rewrite test: totalDataSize={}, halfBudget={}, targetSize={}",
        totalDataSize,
        halfBudget,
        targetSize);

    // Use SparkActions directly instead of ops.rewriteDataFiles() because this test requires
    // fine-grained control over budget options (MAX_TOTAL_FILES_SIZE_BYTES, target-file-size-bytes)
    // that are not exposed through the Operations API.
    RewriteDataFiles.Result result =
        SparkActions.get(ops.spark())
            .rewriteDataFiles(table)
            .binPack()
            .option(RewriteDataFiles.MAX_TOTAL_FILES_SIZE_BYTES, Long.toString(halfBudget))
            .option("target-file-size-bytes", Long.toString(targetSize))
            .option("min-file-size-bytes", "1")
            .option("max-file-size-bytes", Long.toString(targetSize * 2))
            .option("min-input-files", "1")
            .option("delete-file-threshold", "0")
            .execute();

    // Budget covers exactly half the data files by length.
    Assertions.assertEquals(2, result.rewrittenDataFilesCount());
    // With length-based grouping, the 2 data files (total size = targetSize) fit in one group
    // and merge into 1 output file. With sizeBytes-based grouping, each file's perceived size
    // would be data_length + totalDeleteSize, far exceeding the target, so they would be
    // placed in separate groups producing 2 output files instead.
    Assertions.assertEquals(1, result.addedDataFilesCount());

    // Verify data correctness: only odd-numbered IDs remain (even IDs were deleted)
    List<Object[]> expected =
        Arrays.asList(row(1, "data_0"), row(3, "data_1"), row(5, "data_2"), row(7, "data_3"));
    List<Object[]> actual = sql("SELECT * FROM %s ORDER BY id ASC", tableName);
    assertThat(actual).containsExactlyElementsOf(expected);
  }

  private void writeEqDeleteRecord(Table table, String delCol, Object delVal) {
    List<Integer> equalityFieldIds = Lists.newArrayList(table.schema().findField(delCol).fieldId());
    Schema eqDeleteRowSchema = table.schema().select(delCol);
    Record record = GenericRecord.create(eqDeleteRowSchema).copy(ImmutableMap.of(delCol, delVal));
    writeEqDeleteRecord(table, equalityFieldIds, eqDeleteRowSchema, record);
  }

  private void writeEqDeleteRecord(
      Table table, List<Integer> equalityFieldIds, Schema eqDeleteRowSchema, Record deleteRecord) {
    OutputFileFactory fileFactory =
        OutputFileFactory.builderFor(table, 1, 1).format(FileFormat.ORC).build();
    GenericAppenderFactory appenderFactory =
        new GenericAppenderFactory(
            table.schema(),
            table.spec(),
            ArrayUtil.toIntArray(equalityFieldIds),
            eqDeleteRowSchema,
            null);

    EncryptedOutputFile file = createEncryptedOutputFile(null, fileFactory);

    EqualityDeleteWriter<Record> eqDeleteWriter =
        appenderFactory.newEqDeleteWriter(file, FileFormat.ORC, null);

    try (EqualityDeleteWriter<Record> clsEqDeleteWriter = eqDeleteWriter) {
      clsEqDeleteWriter.write(deleteRecord);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
    table.newRowDelta().addDeletes(eqDeleteWriter.toDeleteFile()).commit();
  }

  private EncryptedOutputFile createEncryptedOutputFile(
      PartitionKey partition, OutputFileFactory fileFactory) {
    if (partition == null) {
      return fileFactory.newOutputFile();
    } else {
      return fileFactory.newOutputFile(partition);
    }
  }

  protected List<Object[]> rowsToJava(List<Row> rows) {
    return rows.stream().map(this::toJava).collect(Collectors.toList());
  }

  private Object[] toJava(Row row) {
    return IntStream.range(0, row.size())
        .mapToObj(
            pos -> {
              if (row.isNullAt(pos)) {
                return null;
              }
              Object value = row.get(pos);
              if (value instanceof Row) {
                return toJava((Row) value);
              } else if (value instanceof scala.collection.Seq) {
                return row.getList(pos);
              } else if (value instanceof scala.collection.Map) {
                return row.getJavaMap(pos);
              } else {
                return value;
              }
            })
        .toArray(Object[]::new);
  }

  protected List<Object[]> sql(String query, Object... args) {
    List<Row> rows = ops.spark().sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return Collections.emptyList();
    }
    return rowsToJava(rows);
  }

  protected Object[] row(Object... values) {
    return values;
  }
}

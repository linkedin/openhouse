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
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class MinimalSparkMoRTest extends OpenHouseSparkITest {
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
              true);

  static Operations ops;

  @BeforeEach
  public void startUp() throws Exception {
    ops = Operations.withCatalog(getSparkSession(), null);
  }

  @AfterAll
  public static void shutdown() throws Exception {
    ops.close();
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
    RewriteDataFiles.Result result = rewriteFunc.apply(ops, table);
    Assertions.assertEquals(1, result.addedDataFilesCount());
    Assertions.assertEquals(1, result.rewrittenDataFilesCount());

    // this asserts the dangling delete problem, since compaction did NOT help to remove the final
    // delete
    IcebergTableStats stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumPositionDeleteFiles() + stats.getNumEqualityDeleteFiles()).isEqualTo(3L);
    assertThat(
            stats.getNumCurrentSnapshotPositionDeleteFiles()
                + stats.getNumCurrentSnapshotEqualityDeleteFiles())
        .isEqualTo(1L);

    // notice that running compaction again with no new commits still does not clean up the dangling
    // delete
    result = rewriteFunc.apply(ops, table);
    stats = ops.collectTableStats(tableName);
    assertThat(
            stats.getNumCurrentSnapshotPositionDeleteFiles()
                + stats.getNumCurrentSnapshotEqualityDeleteFiles())
        .isEqualTo(1L);

    // here we provide a new snapshot to mitigate the dangling delete problem, and run compaction
    // again
    records = Arrays.asList(new SimpleRecord(3, "f"));
    ops.spark()
        .createDataset(records, Encoders.bean(SimpleRecord.class))
        .coalesce(1)
        .writeTo(tableName)
        .append();

    result = rewriteFunc.apply(ops, table);
    Assertions.assertEquals(1, result.addedDataFilesCount());
    Assertions.assertEquals(1, result.rewrittenDataFilesCount());

    // finally the dangling delete is cleaned up
    stats = ops.collectTableStats(tableName);
    assertThat(stats.getNumPositionDeleteFiles() + stats.getNumEqualityDeleteFiles()).isEqualTo(3L);
    assertThat(
            stats.getNumCurrentSnapshotPositionDeleteFiles()
                + stats.getNumCurrentSnapshotEqualityDeleteFiles())
        .isEqualTo(0L);
  }
}

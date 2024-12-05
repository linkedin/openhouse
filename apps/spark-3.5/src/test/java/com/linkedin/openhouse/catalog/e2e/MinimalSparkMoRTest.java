package com.linkedin.openhouse.catalog.e2e;

import static org.assertj.core.api.Assertions.*;

import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.jobs.util.SimpleRecord;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.StreamSupport;
import org.apache.iceberg.MetadataTableType;
import org.apache.iceberg.MetadataTableUtils;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

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
              10);

  static Operations ops;

  @BeforeAll
  public static void startUp() throws Exception {
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

    // TODO: put this in the tablestats API
    ops.spark().sql(String.format("DESCRIBE %s", tableName)).show();
    Table table = ops.getTable(tableName);
    Table deleteFilesTable =
        MetadataTableUtils.createMetadataTableInstance(table, MetadataTableType.DELETE_FILES);
    long deleteFileCount =
        StreamSupport.stream(deleteFilesTable.newScan().planFiles().spliterator(), false).count();

    assertThat(deleteFileCount).isEqualTo(3L);

    // 2. Verify the remaining rows are correct after all deletes
    List<Object[]> expected = Arrays.asList(row(1, "b"), row(2, "e"));
    List<Object[]> actual = sql("SELECT * FROM %s ORDER BY id ASC", tableName);
    assertThat(actual).containsExactlyElementsOf(expected);
  }
}

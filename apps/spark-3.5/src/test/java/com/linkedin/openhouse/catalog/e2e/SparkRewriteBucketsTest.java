package com.linkedin.openhouse.catalog.e2e;

import static org.assertj.core.api.Assertions.*;

import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Collections;
import java.util.List;
import org.apache.iceberg.Table;
import org.apache.iceberg.actions.RewriteDataFiles;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.spark.actions.SparkActions;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.analysis.NoSuchTableException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class SparkRewriteBucketsTest extends OpenHouseSparkITest {
  static final String tableName = "db.test_data_compaction_filter";
  private Operations ops;

  @BeforeEach
  public void setUp() throws Exception {
    ops = Operations.withCatalog(getSparkSession(), null);
  }

  @AfterEach
  public void cleanUp() throws Exception {
    sql("DROP TABLE IF EXISTS %s", tableName);
  }

  @Test
  public void testBucketPartitionsCanBeFilteredInCompaction() throws NoSuchTableException {
    SparkSession spark = ops.spark();
    sql(
        "CREATE TABLE openhouse.%s (id int, key string) PARTITIONED BY (bucket(2, key))",
        tableName);
    sql(
        "INSERT INTO openhouse.%s VALUES (0, 'a'), (1, 'b'), (2, 'c'), (3, 'd'), (4, 'e')",
        tableName);
    sql(
        "INSERT INTO openhouse.%s VALUES (5, 'a'), (6, 'b'), (7, 'c'), (8, 'd'), (9, 'e')",
        tableName);

    Table table = ops.getTable(tableName);

    RewriteDataFiles.Result result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .filter(Expressions.in(Expressions.bucket("key", 2), 0))
            .binPack()
            .option("min-input-files", "2")
            .execute();

    // rewrite bucket 0
    assertThat(result.rewrittenDataFilesCount()).isEqualTo(2);
    assertThat(result.addedDataFilesCount()).isEqualTo(1);

    table.refresh();

    result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .filter(Expressions.in(Expressions.bucket("key", 2), 1))
            .binPack()
            .option("min-input-files", "2")
            .execute();

    // rewrite bucket 1
    assertThat(result.rewrittenDataFilesCount()).isEqualTo(2);
    assertThat(result.addedDataFilesCount()).isEqualTo(1);

    table.refresh();

    result =
        SparkActions.get(spark)
            .rewriteDataFiles(table)
            .filter(Expressions.in(Expressions.bucket("key", 2), 1))
            .binPack()
            .option("min-input-files", "2")
            .execute();

    // rewrite bucket 1 and check no-op
    assertThat(result.rewrittenDataFilesCount()).isEqualTo(0);
    assertThat(result.addedDataFilesCount()).isEqualTo(0);
  }

  protected List<Row> sql(String query, Object... args) {
    List<Row> rows = ops.spark().sql(String.format(query, args)).collectAsList();
    if (rows.isEmpty()) {
      return Collections.emptyList();
    }
    return rows;
  }
}

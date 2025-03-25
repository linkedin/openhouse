package com.linkedin.openhouse.datalayout.datasource;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class TableSnapshotStatsTest extends OpenHouseSparkITest {
  @Test
  public void testTableSnapshotStats() throws Exception {
    final String testTable = "db.test_table_snapshot_stats";
    try (SparkSession spark = getSparkSession()) {
      spark.sql("USE openhouse");
      spark.sql(String.format("CREATE TABLE %s (id INT, data STRING)", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (0, '0')", testTable));
      spark.sql(String.format("INSERT INTO %s VALUES (1, '1')", testTable));
      TableSnapshotStats tableSnapshotStats =
          TableSnapshotStats.builder().spark(spark).tableName(testTable).build();
      List<SnapshotStat> stats = tableSnapshotStats.get().collectAsList();
      Assertions.assertEquals(2, stats.size());
      for (SnapshotStat stat : stats) {
        Assertions.assertEquals("append", stat.operation);
        // verify that parsed committed timestamp is within 10 seconds of current time
        Assertions.assertTrue(
            System.currentTimeMillis() - TimeUnit.SECONDS.toMillis(10) <= stat.committedAt
                && stat.committedAt <= System.currentTimeMillis());
      }
    }
  }
}

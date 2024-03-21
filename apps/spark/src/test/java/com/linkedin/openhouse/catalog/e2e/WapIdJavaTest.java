package com.linkedin.openhouse.catalog.e2e;

import static org.apache.iceberg.types.Types.NestedField.*;

import com.linkedin.openhouse.jobs.spark.Operations;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class WapIdJavaTest extends OpenHouseSparkITest {

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
  void testWapWorkflow() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      spark.sql("CREATE TABLE openhouse.d1.t1 (name string)");
      Operations operations = Operations.withCatalog(spark, null);
      Table table = operations.getTable("d1.t1");

      table.newAppend().appendFile(FILE_A).commit();
      long snapshotIdMain = table.currentSnapshot().snapshotId();
      table.newAppend().appendFile(FILE_B).set("wap.id", "wap1").stageOnly().commit();
      List<Snapshot> snapshotList = new ArrayList<>();
      Iterator<Snapshot> it = table.snapshots().iterator();
      while (it.hasNext()) {
        snapshotList.add(it.next());
      }

      // verify that there is one committed snapshot and one staged snapshot
      Assertions.assertEquals(snapshotIdMain, table.currentSnapshot().snapshotId());
      Assertions.assertEquals(
          snapshotIdMain, table.refs().get(SnapshotRef.MAIN_BRANCH).snapshotId());
      Assertions.assertEquals(2, snapshotList.size());

      spark.sql("DROP TABLE openhouse.d1.t1");
    }
  }
}

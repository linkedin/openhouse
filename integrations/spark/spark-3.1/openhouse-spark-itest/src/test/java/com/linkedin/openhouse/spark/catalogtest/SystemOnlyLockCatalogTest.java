package com.linkedin.openhouse.spark.catalogtest;

import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.gen.tables.client.model.LockState;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.spark.OpenHouseCatalog;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SystemOnlyLockCatalogTest extends OpenHouseSparkITest {
  private static final String DATABASE = "system_only_catalog";

  @Test
  void systemActionCatalogCanExpireSnapshotsOnSystemOnlyLockedTable() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      OpenHouseCatalog catalog = (OpenHouseCatalog) getOpenHouseCatalog(spark);
      TableIdentifier id = TableIdentifier.of(DATABASE, "expire_snapshots");
      Table table =
          catalog.createTable(
              id, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
      append(table, "first");
      long firstSnapshot = table.currentSnapshot().snapshotId();
      append(table, "second");
      long secondSnapshot = table.currentSnapshot().snapshotId();
      TableApi controls = controls(spark, false);
      TableApi inspection = controls(spark, true);
      controls
          .createLockV1(
              DATABASE,
              id.name(),
              new CreateUpdateLockRequestBody()
                  .locked(true)
                  .reason(CreateUpdateLockRequestBody.ReasonEnum.SYSTEM_ONLY))
          .block();
      try {
        WebClientResponseWithMessageException failure =
            Assertions.assertThrows(
                WebClientResponseWithMessageException.class, () -> catalog.tableExists(id));
        Assertions.assertEquals(423, failure.getStatusCode());
        Assertions.assertTrue(failure.getMessage().contains("SYSTEM_ONLY"));
        Assertions.assertTrue(failure.getMessage().contains(id.toString()));
        Assertions.assertTrue(failure.getMessage().contains("reason-targeted OpenHouse unlock"));
        // Spark SQL loads the table before dropping it. Spark's catalog hasn't cached this table,
        // so the undeclared DROP TABLE reaches the server and gets 423.
        WebClientResponseWithMessageException dropFailure =
            Assertions.assertThrows(
                WebClientResponseWithMessageException.class,
                () -> spark.sql("DROP TABLE openhouse." + id).collect());
        Assertions.assertEquals(423, dropFailure.getStatusCode());
        LockState lock = lock(inspection, id);

        Map<String, String> properties = new HashMap<>(catalog.properties());
        properties.put("action-type", "SYSTEM");
        OpenHouseCatalog maintenance = new OpenHouseCatalog();
        maintenance.setConf(spark.sparkContext().hadoopConfiguration());
        maintenance.initialize("maintenance", properties);
        maintenance
            .loadTable(id)
            .updateProperties()
            .set(TableProperties.MAX_REF_AGE_MS, String.valueOf(Long.MAX_VALUE))
            .commit();
        Assertions.assertEquals(lock, lock(inspection, id));
        maintenance
            .loadTable(id)
            .expireSnapshots()
            .expireSnapshotId(firstSnapshot)
            .cleanExpiredFiles(false)
            .commit();
        Assertions.assertEquals(lock, lock(inspection, id));

        Table expired = maintenance.loadTable(id);
        Assertions.assertEquals(
            String.valueOf(Long.MAX_VALUE),
            expired.properties().get(TableProperties.MAX_REF_AGE_MS));
        Assertions.assertNull(expired.snapshot(firstSnapshot));
        Assertions.assertNotNull(expired.snapshot(secondSnapshot));
      } finally {
        controls.deleteLockByReasonV1(DATABASE, id.name(), "SYSTEM_ONLY").block();
        // Catalog.dropTable calls DELETE without loading the table, unlike Spark SQL DROP TABLE.
        catalog.dropTable(id);
      }
    }
  }

  private static void append(Table table, String name) {
    table
        .newAppend()
        .appendFile(
            DataFiles.builder(PartitionSpec.unpartitioned())
                .withPath("/path/to/data-" + name + ".parquet")
                .withFileSizeInBytes(10)
                .withRecordCount(1)
                .build())
        .commit();
  }

  private static LockState lock(TableApi inspection, TableIdentifier id) {
    return inspection.getTableV1(DATABASE, id.name()).block().getPolicies().getLockState();
  }

  private TableApi controls(SparkSession spark, boolean systemAction) {
    ApiClient client = new ApiClient();
    client.setBasePath(getOpenHouseLocalServerURI().toString());
    client.addDefaultHeader(
        "Authorization", "Bearer " + spark.conf().get("spark.sql.catalog.openhouse.auth-token"));
    if (systemAction) {
      client.addDefaultHeader("X-OpenHouse-Action-Type", "SYSTEM");
    }
    return new TableApi(client);
  }
}

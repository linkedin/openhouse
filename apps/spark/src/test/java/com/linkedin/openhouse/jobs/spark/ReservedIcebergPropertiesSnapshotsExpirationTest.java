package com.linkedin.openhouse.jobs.spark;

import com.linkedin.openhouse.common.metrics.DefaultOtelConfig;
import com.linkedin.openhouse.common.metrics.OtelEmitter;
import com.linkedin.openhouse.jobs.util.AppsOtelEmitter;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.Arrays;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.exceptions.BadRequestException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * Snapshot expiration where Iceberg table properties are preserved, as at LinkedIn. Runs only in
 * the {@code testWithReservedIcebergProperties} task, whose embedded tables-service sets {@code
 * cluster.tables.preserved-iceberg-properties.enabled} and so uses {@code
 * IcebergPropertiesPreservedKeyChecker}. #708's snapshot expiration failed here: it wrote
 * history.expire.max-ref-age-ms from the client on tables without it.
 */
public class ReservedIcebergPropertiesSnapshotsExpirationTest extends OpenHouseSparkITest {
  private static final String DATABASE = "db_reserved_iceberg_properties";

  private final OtelEmitter otelEmitter =
      new AppsOtelEmitter(Arrays.asList(DefaultOtelConfig.getOpenTelemetry()));

  @Test
  public void expiresTableCommittedWithoutMaxRefAge() throws Exception {
    String tableId = "se_" + UUID.randomUUID().toString().replace("-", "");
    String tableName = DATABASE + "." + tableId;
    try (Operations ops = Operations.withCatalog(getSparkSession(), otelEmitter)) {
      ops.spark().sql(String.format("CREATE TABLE %s (data string)", tableName));
      ops.spark().sql(String.format("INSERT INTO %s VALUES ('stale')", tableName));
      Table table = ops.getTable(tableName);
      long staleSnapshotId = table.currentSnapshot().snapshotId();
      ops.spark().sql(String.format("INSERT INTO %s VALUES ('current')", tableName));
      table.refresh();
      table
          .manageSnapshots()
          .createBranch("stale", staleSnapshotId)
          .setMaxRefAgeMs("stale", 1)
          .commit();

      // Fails fast if this runs without reserved Iceberg properties.
      Assertions.assertThrows(
          BadRequestException.class,
          () ->
              table
                  .updateProperties()
                  .set(TableProperties.MAX_REF_AGE_MS, String.valueOf(TimeUnit.DAYS.toMillis(1)))
                  .commit());

      removeCommittedTableProperty(DATABASE, tableId, TableProperties.MAX_REF_AGE_MS);
      table.refresh();
      Assertions.assertNull(table.properties().get(TableProperties.MAX_REF_AGE_MS));

      ops.expireSnapshots(table, 3, "DAYS", 0);
      table.refresh();

      Assertions.assertFalse(table.refs().containsKey("stale"));
      // Tables-service added the property to snapshot expiration's commit.
      Assertions.assertEquals(
          String.valueOf(TimeUnit.DAYS.toMillis(7)),
          table.properties().get(TableProperties.MAX_REF_AGE_MS));
      ops.spark().sql("DROP TABLE " + tableName);
    }
  }
}

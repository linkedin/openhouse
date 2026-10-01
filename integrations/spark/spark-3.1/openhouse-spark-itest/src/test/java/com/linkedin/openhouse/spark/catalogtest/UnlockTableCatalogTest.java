package com.linkedin.openhouse.spark.catalogtest;

import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.gen.tables.client.model.LockState;
import com.linkedin.openhouse.gen.tables.client.model.Policies;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ValidationException;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class UnlockTableCatalogTest extends OpenHouseSparkITest {
  private static final String DATABASE = "unlock_catalog";

  @Test
  void sqlUnlockRemovesOnlyTheMatchingLock() throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      TableIdentifier id = TableIdentifier.of(DATABASE, "matching_lock");
      catalog.createTable(
          id, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
      String table = "openhouse." + id;
      TableApi controls = controls(spark);
      try {
        lock(controls, id, CreateUpdateLockRequestBody.ReasonEnum.SYSTEM_ONLY);
        assertUnlockFails(spark, "ALTER TABLE " + table + " UNLOCK", 409, "reason-targeted");
        Assertions.assertEquals(
            423,
            Assertions.assertThrows(
                    WebClientResponseWithMessageException.class, () -> catalog.loadTable(id))
                .getStatusCode());

        // Unlocking must not load the table, which SYSTEM_ONLY blocks.
        spark.sql("ALTER TABLE " + table + " UNLOCK REASON system_only");
        Assertions.assertNull(lockState(controls, id));
        Assertions.assertNotNull(catalog.loadTable(id));

        lock(controls, id, null);
        Assertions.assertTrue(policiesProperty(spark, table).contains("LEGACY"));
        assertUnlockFails(
            spark, "ALTER TABLE " + table + " UNLOCK REASON SYSTEM_ONLY", 409, "does not match");
        assertUnlockFails(spark, "ALTER TABLE " + table + " UNLOCK REASON UNKNOWN", 400, null);
        for (String reason : new String[] {"``", "` `"}) {
          Assertions.assertThrows(
              IllegalArgumentException.class,
              () -> spark.sql("ALTER TABLE " + table + " UNLOCK REASON " + reason));
        }
        Assertions.assertThrows(
            ValidationException.class,
            () ->
                spark.sql("ALTER TABLE openhouse." + DATABASE + ".extra." + id.name() + " UNLOCK"));
        Assertions.assertEquals(LockState.ReasonEnum.LEGACY, lockState(controls, id).getReason());
        spark.sql("ALTER TABLE " + table + " UNLOCK");
        Assertions.assertNull(lockState(controls, id));
        // The session must not keep showing the removed lock.
        Assertions.assertFalse(policiesProperty(spark, table).contains("LEGACY"));

        lock(controls, id, null);
        spark.sql("ALTER TABLE " + table + " UNLOCK REASON legacy");
        Assertions.assertNull(lockState(controls, id));

        // Unlocking an unlocked table is a no-op.
        spark.sql("ALTER TABLE " + table + " UNLOCK");
        spark.sql("ALTER TABLE " + table + " UNLOCK REASON SYSTEM_ONLY");
      } finally {
        for (String reason : new String[] {"SYSTEM_ONLY", "LEGACY"}) {
          try {
            controls.deleteLockByReasonV1(DATABASE, id.name(), reason).block();
          } catch (RuntimeException e) {
            // Only the leftover lock's reason can remove it.
          }
        }
        catalog.dropTable(id);
      }
    }
  }

  private static void assertUnlockFails(
      SparkSession spark, String sql, int status, String message) {
    WebClientResponseWithMessageException failure =
        Assertions.assertThrows(WebClientResponseWithMessageException.class, () -> spark.sql(sql));
    Assertions.assertEquals(status, failure.getStatusCode(), failure.getMessage());
    if (message != null) {
      Assertions.assertTrue(failure.getMessage().contains(message), failure.getMessage());
    }
  }

  private static String policiesProperty(SparkSession spark, String table) {
    return spark
        .sql("SHOW TBLPROPERTIES " + table + " ('policies')")
        .collectAsList()
        .get(0)
        .getString(1);
  }

  private static void lock(
      TableApi controls, TableIdentifier id, CreateUpdateLockRequestBody.ReasonEnum reason) {
    controls
        .createLockV1(
            DATABASE, id.name(), new CreateUpdateLockRequestBody().locked(true).reason(reason))
        .block();
  }

  private static LockState lockState(TableApi controls, TableIdentifier id) {
    Policies policies = controls.getTableV1(DATABASE, id.name()).block().getPolicies();
    return policies == null ? null : policies.getLockState();
  }

  private TableApi controls(SparkSession spark) {
    ApiClient client = new ApiClient();
    client.setBasePath(getOpenHouseLocalServerURI().toString());
    client.addDefaultHeader(
        "Authorization", "Bearer " + spark.conf().get("spark.sql.catalog.openhouse.auth-token"));
    return new TableApi(client);
  }
}

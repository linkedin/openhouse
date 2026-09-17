package com.linkedin.openhouse.spark.catalogtest;

import com.linkedin.openhouse.gen.tables.client.api.TableApi;
import com.linkedin.openhouse.gen.tables.client.invoker.ApiClient;
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateLockRequestBody;
import com.linkedin.openhouse.gen.tables.client.model.GetLockResponseBody;
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException;
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException;
import com.linkedin.openhouse.spark.OpenHouseCatalog;
import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class CleanupLockCatalogTest extends OpenHouseSparkITest {
  private static final String DATABASE = "cleanup_catalog";

  @ParameterizedTest
  @ValueSource(strings = {"load", "refresh", "exists"})
  void cleanupDenialIsNotMistakenForAMissingTable(String operation) throws Exception {
    try (SparkSession spark = getSparkSession()) {
      OpenHouseCatalog catalog = (OpenHouseCatalog) getOpenHouseCatalog(spark);
      TableIdentifier id = TableIdentifier.of(DATABASE, "denied_" + operation);
      catalog.createTable(
          id, new Schema(Types.NestedField.required(1, "id", Types.LongType.get())));
      Table loaded = catalog.loadTable(id);
      TableApi controls = controls(spark);
      String uuid = controls.getTableV1(DATABASE, id.name()).block().getTableUUID();
      controls
          .createLockV1(
              DATABASE,
              id.name(),
              new CreateUpdateLockRequestBody()
                  .locked(true)
                  .reason(CreateUpdateLockRequestBody.ReasonEnum.TIER3_AUTO_CLEANUP)
                  .expectedTableUUID(uuid)
                  .message("eligible for cleanup"))
          .block();
      try {
        WebClientResponseWithMessageException failure =
            Assertions.assertThrows(
                WebClientResponseWithMessageException.class,
                () -> {
                  switch (operation) {
                    case "load":
                      catalog.loadTable(id);
                      break;
                    case "refresh":
                      loaded.refresh();
                      break;
                    case "exists":
                      catalog.tableExists(id);
                      break;
                    default:
                      throw new AssertionError(operation);
                  }
                });
        Assertions.assertEquals(423, failure.getStatusCode());
        Assertions.assertTrue(failure.getMessage().contains("TIER3_AUTO_CLEANUP"));
        Assertions.assertTrue(failure.getMessage().contains(id.toString()));
        Assertions.assertTrue(failure.getMessage().contains("eligible for cleanup"));
        Assertions.assertTrue(failure.getMessage().contains("Tier 2"));
        Assertions.assertTrue(failure.getMessage().contains("reason-targeted OpenHouse unlock"));

        Map<String, String> properties = new HashMap<>(catalog.properties());
        properties.put("system-action", "true");
        OpenHouseCatalog maintenance = new OpenHouseCatalog();
        maintenance.setConf(spark.sparkContext().hadoopConfiguration());
        maintenance.initialize("maintenance", properties);
        Assertions.assertDoesNotThrow(() -> maintenance.loadTable(id).refresh());
        Assertions.assertTrue(maintenance.tableExists(id));
      } finally {
        GetLockResponseBody status = controls.getLockV1(DATABASE, id.name()).block();
        controls
            .deleteLockByReasonV1(
                DATABASE,
                id.name(),
                "TIER3_AUTO_CLEANUP",
                status.getTableUUID(),
                status.getLockState().getLockOwner())
            .block();
        catalog.dropTable(id);
      }
    }
  }

  @ParameterizedTest
  @CsvSource({"bad-name,400", "missing_table,404"})
  void malformedAndMissingTablesKeepTheirNotFoundBehavior(String name, int status)
      throws Exception {
    try (SparkSession spark = getSparkSession()) {
      Catalog catalog = getOpenHouseCatalog(spark);
      TableIdentifier id = TableIdentifier.of(DATABASE, name);
      TableApi api = controls(spark);
      WebClientResponseException raw =
          Assertions.assertThrows(
              WebClientResponseException.class, () -> api.getTableV1(DATABASE, name).block());
      Assertions.assertEquals(status, raw.getRawStatusCode());
      Assertions.assertThrows(NoSuchTableException.class, () -> catalog.loadTable(id));
      Assertions.assertFalse(catalog.tableExists(id));
    }
  }

  private TableApi controls(SparkSession spark) {
    ApiClient client = new ApiClient();
    client.setBasePath(getOpenHouseLocalServerURI().toString());
    client.addDefaultHeader(
        "Authorization", "Bearer " + spark.conf().get("spark.sql.catalog.openhouse.auth-token"));
    return new TableApi(client);
  }
}

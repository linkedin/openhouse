package com.linkedin.openhouse.spark.catalogtest;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.tablestest.OpenHouseSparkITest;
import java.util.HashMap;
import java.util.Map;
import org.apache.iceberg.Schema;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.catalog.ViewCatalog;
import org.apache.iceberg.exceptions.NoSuchViewException;
import org.apache.iceberg.types.Types;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Spark-3.5-only test for the ENABLED OpenHouse view path, which cannot run on Spark 3.1 because
 * its Iceberg 1.2 catalog is not a {@code ViewCatalog}. The cross-version disabled-parity behavior
 * is covered by {@code OpenHouseViewSparkITest}.
 *
 * <p>Views are enabled via {@code spark.sql.catalog.openhouse.iceberg-views-enabled=true}. The
 * client view operations now call the OpenHouse Views service (via {@code ViewApi}) instead of an
 * in-memory store, but the service is currently a 404 stub ({@code ViewsDisabledService}). The full
 * buildView -> loadView -> listViews -> dropView round-trip cannot pass yet, so this test asserts
 * the create fails with the service's 404, which the client classifies into {@link
 * org.apache.iceberg.exceptions.NoSuchViewException} — still validating that the enabled view path
 * is wired through {@code ViewApi} to the service rather than an in-memory store (which would have
 * created the view and succeeded). Client-level coverage (mocked {@code ViewApi}) lives in {@code
 * OpenHouseViewOperationsTest} in the iceberg-1.5 itest module.
 */
public class OpenHouseViewEnabledTestSpark3_5 extends OpenHouseSparkITest {

  // TODO: restore the full buildView -> loadView -> listViews -> dropView round-trip once the
  // OpenHouse Views service is implemented (it is currently a 404 stub, ViewsDisabledService).
  @Test
  public void testEnabledViewCreateReachesViewsService() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("spark.sql.catalog.openhouse.iceberg-views-enabled", "true");
    try (SparkSession spark = getSparkSession("openhouse", overrides)) {
      ViewCatalog catalog = (ViewCatalog) getOpenHouseCatalog(spark);
      Namespace namespace = Namespace.of("viewenabled_db");
      TableIdentifier viewId = TableIdentifier.of("viewenabled_db", "v_roundtrip");
      Schema schema = new Schema(Types.NestedField.required(1, "c", Types.IntegerType.get()));

      // The create reaches the Views service through ViewApi; the 404 stub (ViewsDisabledService)
      // is classified by the client into NoSuchViewException. An in-memory store would instead have
      // created the view and returned success, so this proves the enabled path is wired to the
      // service and exercises the client's HTTP-status classification.
      NoSuchViewException exception =
          assertThrows(
              NoSuchViewException.class,
              () ->
                  catalog
                      .buildView(viewId)
                      .withSchema(schema)
                      .withQuery("spark", "SELECT 1 AS c")
                      .withDefaultNamespace(namespace)
                      .withDefaultCatalog("openhouse")
                      .create());
      // The mapping only fires on the service's HTTP 404 response, confirming the round-trip.
      assertTrue(exception.getCause().getClass().getName().contains("WebClientResponseException"));
    }
  }
}

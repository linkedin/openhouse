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
import org.apache.iceberg.view.View;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

/**
 * Spark-3.5-only test for the ENABLED OpenHouse view path (the feature's happy path), which cannot
 * run on Spark 3.1 because its Iceberg 1.2 catalog is not a {@code ViewCatalog}. The cross-version
 * disabled-parity behavior is covered by {@link OpenHouseViewSparkITest}.
 *
 * <p>Views are enabled via {@code spark.sql.catalog.openhouse.iceberg-views-enabled=true}. The view
 * operations are exercised programmatically against the Iceberg {@link ViewCatalog} (rather than
 * via Spark SQL {@code CREATE VIEW}, which passes a null location that the in-memory mock backend
 * cannot satisfy yet). This validates the buildView -> loadView -> listViews -> dropView round-trip
 * against the mock backend before the OpenHouse Views service exists.
 */
public class OpenHouseViewEnabledTestSpark3_5 extends OpenHouseSparkITest {

  @Test
  public void testEnabledViewBuildLoadListDropRoundTrip() throws Exception {
    Map<String, String> overrides = new HashMap<>();
    overrides.put("spark.sql.catalog.openhouse.iceberg-views-enabled", "true");
    try (SparkSession spark = getSparkSession("openhouse", overrides)) {
      ViewCatalog catalog = (ViewCatalog) getOpenHouseCatalog(spark);
      Namespace namespace = Namespace.of("viewenabled_db");
      TableIdentifier viewId = TableIdentifier.of("viewenabled_db", "v_roundtrip");
      Schema schema = new Schema(Types.NestedField.required(1, "c", Types.IntegerType.get()));

      View created =
          catalog
              .buildView(viewId)
              .withSchema(schema)
              .withQuery("spark", "SELECT 1 AS c")
              .withDefaultNamespace(namespace)
              .withDefaultCatalog("openhouse")
              .create();
      assertNotNull(created);

      // loadView round-trip returns the persisted (mock) SQL representation.
      View loaded = catalog.loadView(viewId);
      assertEquals("SELECT 1 AS c", loaded.sqlFor("spark").sql());

      // listViews surfaces the created view within its namespace.
      assertTrue(catalog.listViews(namespace).contains(viewId));

      // dropView removes it, after which loadView reports NoSuchViewException.
      assertTrue(catalog.dropView(viewId));
      assertThrows(NoSuchViewException.class, () -> catalog.loadView(viewId));
    }
  }
}

package com.linkedin.openhouse.spark;

import java.util.Map;
import org.apache.spark.sql.SparkSession;

/**
 * Catalog implementation to create, read, update and delete tables in OpenHouse. This class
 * leverages Openhouse tableclient to perform CRUD operations on Tables resource in the Catalog
 * service. This implementation provides client side catalog implementation for Iceberg tables in
 * Spark.
 *
 * <p>Catalog can be instantiated as a Iceberg catalog, with following configurations:
 * spark.sql.catalog.openhouse=org.apache.iceberg.spark.SparkCatalog
 * spark.sql.catalog.openhouse.catalog-impl=com.linkedin.openhouse.spark.OpenHouseCatalog
 * spark.sql.catalog.openhouse.metrics-reporter-impl=com.linkedin.openhouse.javaclient.OpenHouseMetricsReporter
 * spark.sql.catalog.openhouse.uri=http://[openhouse service host]:[openhouse service port]
 * spark.sql.catalog.openhouse.cluster=[openhouse cluster name]
 *
 * <p>It can be used in spark shell as follows: spark.sql("USE openhouse")
 *
 * <p>Overrides {@link #initialize} to force {@code spark.sql.caseSensitive=false} in the active
 * Spark session. OpenHouse tables preserve the exact column casing stored in the catalog, so reads
 * must resolve column references case-insensitively regardless of the caller's session
 * configuration. Without this override a user job that sets {@code spark.sql.caseSensitive=true}
 * would fail to resolve columns whose stored name differs in case from the reference in the query
 * (e.g. querying {@code id} on a table whose schema stores {@code ID}).
 */
public class OpenHouseCatalog extends com.linkedin.openhouse.javaclient.OpenHouseCatalog {

  @Override
  public void initialize(String name, Map<String, String> properties) {
    super.initialize(name, properties);
    // Ensure case-insensitive column resolution for all reads and writes against OH tables.
    // Spark's default is false, but users can override it. We force it back to false so that
    // column references in queries and views resolve against the table's stored casing regardless
    // of what the caller has configured.
    SparkSession.active().conf().set("spark.sql.caseSensitive", "false");
  }
}

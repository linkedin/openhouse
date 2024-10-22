package com.linkedin.openhouse.spark;

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
 */
public class OpenHouseCatalog extends com.linkedin.openhouse.javaclient.OpenHouseCatalog {}

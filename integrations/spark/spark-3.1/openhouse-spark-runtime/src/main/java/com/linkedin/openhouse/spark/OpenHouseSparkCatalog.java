package com.linkedin.openhouse.spark;

import com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper.IcebergCatalogMapper;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.spark.Spark3Util;
import org.apache.iceberg.spark.SparkCatalog;
import org.apache.spark.sql.connector.catalog.Identifier;

public class OpenHouseSparkCatalog extends SparkCatalog {

  // Delegate purgeTable to the Iceberg catalog
  @Override
  public boolean purgeTable(Identifier ident) {
    Catalog catalog = IcebergCatalogMapper.toIcebergCatalog(this);
    if (catalog != null) {
      return catalog.dropTable(Spark3Util.identifierToTableIdentifier(ident), true);
    } else {
      return super.purgeTable(ident);
    }
  }
}

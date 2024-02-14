package com.linkedin.openhouse.spark.sql.execution.datasources.v2.mapper

import org.apache.iceberg.CachingCatalog
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.common.DynFields
import org.apache.iceberg.spark.{SparkCatalog, SparkSessionCatalog}
import org.apache.spark.sql.connector.catalog.TableCatalog

object IcebergCatalogMapper {

  /**
   * Convert Spark's {@link TableCatalog} to Iceberg's {@link Catalog}
   *
   * {@link Catalog} instance is a private field inside of a chain of wrapping {@link Catalog} classes in {@link TableCatalog}.
   * To access the instance we need to access following private fields:
   *    {@link SparkSessionCatalog#icebergCatalog} -> {@link SparkCatalog}
   *    {@link SparkCatalog#icebergCatalog} -> {@link CachingCatalog}
   *    {@link CachingCatalog#catalog} -> {@link OpenHouseCatalog}
   *
   * @return null :if it is not iceberg based catalog
   *         catalog :if iceberg catalog
   */
  def toIcebergCatalog(catalog: TableCatalog): Catalog = {
    if (!(catalog.isInstanceOf[SparkCatalog] || catalog.isInstanceOf[SparkSessionCatalog[_]])) {
      null
    } else {
      val sparkCatalog = if (catalog.isInstanceOf[SparkSessionCatalog[_]]) {
        DynFields.builder.hiddenImpl(classOf[SparkSessionCatalog[_]], "icebergCatalog").build[TableCatalog](catalog).get
      } else {
        catalog
      }
      var icebergCatalog = DynFields.builder.hiddenImpl(classOf[SparkCatalog], "icebergCatalog").build[Catalog](sparkCatalog).get
      val cacheEnabled = DynFields.builder.hiddenImpl(classOf[SparkCatalog], "cacheEnabled").build[Boolean](sparkCatalog).get
      if (cacheEnabled) icebergCatalog = DynFields.builder.hiddenImpl(classOf[CachingCatalog], "catalog").build[Catalog](icebergCatalog).get
      icebergCatalog
    }
  }
}

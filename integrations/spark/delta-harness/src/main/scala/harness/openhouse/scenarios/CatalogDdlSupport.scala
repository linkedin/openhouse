package harness

import org.apache.spark.sql.SparkSession

trait CatalogDdlSupport extends ScenarioKit {

  protected final def icebergTableOf(spark: SparkSession, table: String): org.apache.iceberg.Table =
    org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark, table)

}

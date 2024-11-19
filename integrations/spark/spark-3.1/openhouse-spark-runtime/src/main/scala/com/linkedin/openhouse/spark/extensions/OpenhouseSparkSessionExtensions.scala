package com.linkedin.openhouse.spark.extensions

import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseSparkSqlExtensionsParser
import com.linkedin.openhouse.spark.sql.execution.datasources.v2.OpenhouseDataSourceV2Strategy
import org.apache.spark.sql.SparkSessionExtensions

class OpenhouseSparkSessionExtensions extends (SparkSessionExtensions => Unit) {
  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectParser { case (_, parser) => new OpenhouseSparkSqlExtensionsParser(parser) }
    extensions.injectPlannerStrategy( spark => OpenhouseDataSourceV2Strategy(spark))
  }
}

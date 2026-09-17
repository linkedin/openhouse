package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import org.apache.iceberg.spark.source.SparkTable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.execution.datasources.v2.LeafV2CommandExec

case class SetRetentionPolicyExec(
  catalog: TableCatalog,
  ident: Identifier,
  granularity: String,
  count: Int,
  colName: Option[String],
  colPattern: Option[String],
  timeZone: Option[String]
                                 ) extends LeafV2CommandExec {

  override lazy val output: Seq[Attribute] = Nil

  override protected def run(): Seq[InternalRow] = {
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        val key = "updated.openhouse.policy"
        val timeZoneJson = timeZone match {
          case Some(tz) => s""","timeZone":"${escapeJson(tz)}""""
          case None => ""
        }
        val value = {
          (colName, colPattern) match {
            case (None, None) => s"""{"retention":{"count":${count},"granularity":"${granularity}"${timeZoneJson}}}"""
            case (Some(nameVal), Some(patternVal)) => {
              val columnPattern = s"""{"columnName":"${escapeJson(nameVal)}","pattern": "${escapeJson(patternVal)}"}"""
              s"""{"retention":{"count":${count},"granularity":"${granularity}"${timeZoneJson}, "columnPattern":${columnPattern}}}"""
            }
            case (Some(nameVal), None) => {
              val columnPattern = s"""{"columnName":"${escapeJson(nameVal)}","pattern": ""}"""
              s"""{"retention":{"count":${count},"granularity":"${granularity}"${timeZoneJson}, "columnPattern":${columnPattern}}}"""
            }
          }
        }

        iceberg.table().updateProperties()
          .set(key, value)
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set retention policy for non-Openhouse table: $table")
    }

    Nil
  }

  /**
   * Escapes a raw string so it can be embedded as a JSON string value. Table owners supply the time
   * zone, column name, and pattern as free-text SQL string literals; without escaping, a value
   * containing a quote or backslash could break out of the policy value and inject additional JSON
   * fields.
   */
  private def escapeJson(raw: String): String = {
    val builder = new StringBuilder(raw.length + 8)
    raw.foreach {
      case '"' => builder.append("\\\"")
      case '\\' => builder.append("\\\\")
      case '\b' => builder.append("\\b")
      case '\f' => builder.append("\\f")
      case '\n' => builder.append("\\n")
      case '\r' => builder.append("\\r")
      case '\t' => builder.append("\\t")
      case controlChar if controlChar < 0x20 => builder.append("\\u%04x".format(controlChar.toInt))
      case other => builder.append(other)
    }
    builder.toString
  }

  override def simpleString(maxFields: Int): String = {
    s"SetRetentionPolicyExec: ${catalog} ${ident} ${count} ${granularity} ${colName.getOrElse("")} ${colPattern.getOrElse("")} ${timeZone.getOrElse("")}"
  }
}

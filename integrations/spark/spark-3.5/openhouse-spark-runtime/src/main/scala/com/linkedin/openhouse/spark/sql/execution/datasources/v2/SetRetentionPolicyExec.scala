package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.fasterxml.jackson.databind.ObjectMapper
import java.time.{DateTimeException, ZoneId}
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
    timeZone.foreach(validateTimeZone)
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        iceberg.table().updateProperties()
          .set("updated.openhouse.policy", retentionPolicyJson)
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set retention policy for non-Openhouse table: $table")
    }

    Nil
  }

  /**
   * Reject an invalid time zone here rather than persisting a policy the retention job
   * cannot resolve. A table owner supplies the zone as free text in the SQL statement.
   */
  private def validateTimeZone(tz: String): Unit = {
    try ZoneId.of(tz)
    catch {
      case cause: DateTimeException =>
        throw new IllegalArgumentException(
          s"Invalid retention time zone '$tz': expected an IANA zone id such as America/Los_Angeles or a fixed offset such as +05:30",
          cause)
    }
  }

  /**
   * Serialize the retention policy with a JSON writer so quotes, backslashes, and control
   * characters in the owner-supplied column name, pattern, and time zone are escaped by the
   * library rather than by hand.
   */
  private def retentionPolicyJson: String = {
    val mapper = new ObjectMapper()
    val retention = mapper.createObjectNode()
    retention.put("count", count)
    retention.put("granularity", granularity)
    timeZone.foreach(tz => retention.put("timeZone", tz))
    colName.foreach { name =>
      val columnPattern = retention.putObject("columnPattern")
      columnPattern.put("columnName", name)
      columnPattern.put("pattern", colPattern.getOrElse(""))
    }
    val policy = mapper.createObjectNode()
    policy.set("retention", retention)
    mapper.writeValueAsString(policy)
  }

  override def simpleString(maxFields: Int): String = {
    s"SetRetentionPolicyExec: ${catalog} ${ident} ${count} ${granularity} ${colName.getOrElse("")} ${colPattern.getOrElse("")} ${timeZone.getOrElse("")}"
  }
}

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
    timeZone.foreach(SetRetentionPolicyExec.validateTimeZone)
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        iceberg.table().updateProperties()
          .set(
            "updated.openhouse.policy",
            SetRetentionPolicyExec.retentionPolicyJson(granularity, count, timeZone, colName, colPattern))
          .commit()

      case table =>
        throw new UnsupportedOperationException(s"Cannot set retention policy for non-Openhouse table: $table")
    }

    Nil
  }

  override def simpleString(maxFields: Int): String = {
    s"SetRetentionPolicyExec: ${catalog} ${ident} ${count} ${granularity} ${colName.getOrElse("")} ${colPattern.getOrElse("")} ${timeZone.getOrElse("")}"
  }
}

object SetRetentionPolicyExec {

  /**
   * Rejects a time zone that `ZoneId` cannot resolve, so an invalid zone fails the statement
   * instead of persisting a policy the retention job cannot evaluate. A table owner supplies the
   * zone as free text in the SQL statement.
   *
   * The declared exception is unchecked because this runs on the `run` override inherited from
   * `LeafV2CommandExec`, whose signature declares no checked exception, so there is no checked
   * channel to return the failure through; Spark surfaces the throw as the failed statement. This
   * matches the unchecked rejections the sibling policy execs already use for a non-Openhouse table.
   *
   * @throws java.lang.IllegalArgumentException if the zone is not a valid IANA id or fixed offset.
   */
  @throws[IllegalArgumentException]("if the zone is not a valid IANA id or fixed offset")
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
   * Serializes the retention policy with a JSON writer so quotes, backslashes, and control
   * characters in the owner-supplied column name, pattern, and time zone are escaped by the library
   * rather than by hand. Pure: it reads only its arguments.
   */
  private def retentionPolicyJson(
    granularity: String,
    count: Int,
    timeZone: Option[String],
    colName: Option[String],
    colPattern: Option[String]): String = {
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
}

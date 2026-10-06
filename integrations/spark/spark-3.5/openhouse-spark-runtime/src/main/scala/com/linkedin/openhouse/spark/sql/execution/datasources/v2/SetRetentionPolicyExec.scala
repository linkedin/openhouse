package com.linkedin.openhouse.spark.sql.execution.datasources.v2

import com.fasterxml.jackson.databind.ObjectMapper
import com.linkedin.openhouse.spark.sql.catalyst.parser.extensions.OpenhouseParseException
import java.time.{DateTimeException, ZoneId}
import org.apache.iceberg.spark.source.SparkTable
import org.apache.iceberg.types.Types
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

  @throws[OpenhouseParseException]
  override protected def run(): Seq[InternalRow] = {
    timeZone.foreach { declaredTimeZone =>
      try ZoneId.of(declaredTimeZone)
      catch {
        case cause: DateTimeException =>
          throw new OpenhouseParseException(
            s"Invalid retention time zone '$declaredTimeZone': ${cause.getMessage}",
            1, 0)
      }
      if (colName.isEmpty) {
        throw new OpenhouseParseException(
          "WITH TIMEZONE requires ON COLUMN for a string retention column", 1, 0)
      }
      if (colPattern.exists(_.replaceAll("'[^']*'", "")
          .exists(character => "VvzOXxZ".indexOf(character) >= 0))) {
        throw new OpenhouseParseException(
          "The retention column pattern already encodes a time zone", 1, 0)
      }
    }
    catalog.loadTable(ident) match {
      case iceberg: SparkTable if iceberg.table().properties().containsKey("openhouse.tableId") =>
        if (timeZone.isDefined &&
            !colName.flatMap(name => Option(iceberg.table().schema().findType(name)))
              .contains(Types.StringType.get())) {
          throw new OpenhouseParseException(
            "WITH TIMEZONE requires a string retention column", 1, 0)
        }
        val mapper = new ObjectMapper()
        val retention = mapper.createObjectNode()
        retention.put("count", count)
        retention.put("granularity", granularity)
        timeZone.foreach(declaredTimeZone => retention.put("timeZone", declaredTimeZone))
        colName.foreach { columnName =>
          val columnPattern = retention.putObject("columnPattern")
          columnPattern.put("columnName", columnName)
          columnPattern.put("pattern", colPattern.getOrElse(""))
        }
        val policy = mapper.createObjectNode()
        policy.set("retention", retention)
        iceberg.table().updateProperties()
          .set("updated.openhouse.policy", mapper.writeValueAsString(policy))
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

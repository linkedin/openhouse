package harness

import org.apache.spark.sql.{AnalysisException, Row, SparkSession}
import org.apache.iceberg.exceptions.BadRequestException
import org.apache.iceberg.exceptions.ValidationException
import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import scala.annotation.tailrec
import scala.reflect.{ClassTag, classTag}
import scala.util.control.NonFatal

// The RTAS hazard family. A column tag policy is set on a table, the table is then replaced through
// CREATE OR REPLACE TABLE AS SELECT, and the case reads the policy back. The cases run on parquet
// and orc.
trait RtasHazardScenarios extends RtasScenarioKit { this: HazardReaderWriterScenarios =>
  import Rows._

  def hazardRtasCases(format: String): List[Plan.Case] = {
    val taggedReplacePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table => cowCreate(table, format))()
        .insert(3)()
        .sql("enableReplace")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('replace.enabled'='true')")()
        .sql("tagPii")(table =>
          s"ALTER TABLE $table MODIFY COLUMN " +
            s"${Core.string0.columnName} SET TAG = (PII)")(),
      description = s"Three seed rows in a $format table with replace.enabled set and the string " +
        "column tagged PII.")

    List(
      taggedReplacePreparation.test(
        "hazard.rtas.preservesColumnTags",
        "CREATE OR REPLACE TABLE AS SELECT preserves the PII column tag policy that was set " +
          "before the replace.") { table =>
        val policiesBefore =
          tableProps(table.spark, table.name).getOrElse("policies", "")
        assert(
          policiesBefore.toLowerCase.contains("pii") ||
            policiesBefore.toLowerCase.contains("columntags"),
          s"PII tag was not stored before RTAS: $policiesBefore")

        table.spark.sql(
          s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
            s"AS SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} <= 2")
        val policiesAfter =
          tableProps(table.spark, table.name).getOrElse("policies", "")

        assert(
          policiesAfter == policiesBefore,
          s"RTAS should preserve the PII column tag: $policiesAfter")
      })
  }
}

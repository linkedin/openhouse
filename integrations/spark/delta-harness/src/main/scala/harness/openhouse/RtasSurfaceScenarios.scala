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

// The RTAS surface families. One case reads back the message the catalog returns when replace is
// disabled, alongside the other rejection messages a user meets; the other races a replace against
// an append. Both drive CREATE OR REPLACE TABLE AS SELECT, so they belong to the RTAS layer. The
// seeded preparation and the concurrency helpers come from the standard surface trait. The cases run
// on parquet and orc.
trait RtasSurfaceScenarios extends RtasScenarioKit { this: SurfaceScenarios =>
  import Rows._

  // A rejection a SQL user reads back is readable when the message is non-empty, carries no
  // internal-error marker, carries no raw stack frames, and starts with something other than
  // java.lang.NullPointerException.
  private def assertReadableMessage(context: String)(e: Throwable): Unit = {
    val m = Option(e.getMessage).getOrElse("")
    assert(m.nonEmpty, s"$context: empty error message (worst possible readability)")
    assert(!m.contains("[INTERNAL_ERROR]"), s"$context: internal error surfaced to the user: ${m.take(160)}")
    assert(!m.contains("\n\tat ") && !m.contains("\tat java."), s"$context: stacktrace frames in the user-facing message: ${m.take(160)}")
    assert(!m.startsWith("java.lang.NullPointerException"), s"$context: bare NPE surfaced: ${m.take(160)}")
  }

  private def surfaceReplacePreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)()
        .sql("enableReplace")(table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('replace.enabled'='true')")(),
      description = s"Three seed rows in an unpartitioned $format table with " +
        "replace.enabled=true.")

  // The rejection messages a user reads back from the catalog.
  def surfaceMessageCases(format: String): List[Plan.Case] =
    List(
      surfaceBasePreparation(format).test(
        "surface.msg.readabilityGuard",
        "Rejection messages for a dropped column, a reserved property, disabled RTAS and " +
          "CREATE NAMESPACE are all non-empty, free of internal-error markers, free of raw " +
          "stack frames, and not a bare NullPointerException.") { table =>
        assertReadableMessage("dropColumn")(
          Check.intercept[Exception](
            table.spark.sql(
              s"ALTER TABLE ${table.name} " +
                s"DROP COLUMN ${Core.int0.columnName}")))
        assertReadableMessage("reservedProp")(
          Check.intercept[Exception](
            table.spark.sql(
              s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
                "('openhouse.tableUUID'='x')")))
        assertReadableMessage("rtasDisabled")(
          Check.intercept[Exception](
            table.spark.sql(
              s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
                s"AS SELECT * FROM ${table.name}")))
        assertReadableMessage("createNamespace")(
          Check.intercept[Exception](
            table.spark.sql("CREATE NAMESPACE openhouse.nope_ns")))
      })

  // A replace racing an append. The outcome is either a commit or a typed commit conflict.
  def surfaceRtasConcurrencyCases(format: String): List[Plan.Case] =
    List(
      surfaceReplacePreparation(format).test(
        "surface.conc.rtasVsAppend",
        "A concurrent CREATE OR REPLACE TABLE AS SELECT racing an INSERT settles at either 2 " +
          "rows (replace won) or 3 rows (append also landed), with any failure being a typed " +
          "commit conflict.") { table =>
        def replaceTable(): Unit =
          try {
            table.spark.sql(
              s"CREATE OR REPLACE TABLE ${table.name} USING $dataSource " +
                s"AS SELECT * FROM ${table.name} " +
                s"WHERE ${Core.long0.columnName} <= 2")
          } catch {
            case exception: Throwable =>
              assert(
                isTypedCommitConflict(exception),
                s"RTAS race failed with ${exception.getClass.getName}")
          }
        def appendRow(): Unit =
          try {
            table.spark.sql(
              s"INSERT INTO ${table.name} VALUES " +
                "(CAST(30 AS BIGINT), 30, 'row-30', 30.5, " +
                "true, '2024-01-09-01')")
          } catch {
            case exception: Throwable =>
              assert(
                isTypedCommitConflict(exception),
                s"append race failed with ${exception.getClass.getName}")
          }
        val threadErrors =
          runConcurrently(Seq(() => replaceTable(), () => appendRow()))

        assert(
          threadErrors.isEmpty,
          s"racing thread failed with a non-conflict error: $threadErrors")
        table.spark.sql(s"REFRESH TABLE ${table.name}")
        val rowCount = countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name}").toLong
        assert(
          rowCount == 2 || rowCount == 3,
          s"RTAS and append race settled at $rowCount rows")
        println(s"DIAG conc.rtasVsAppend: settled at $rowCount rows")
      })
}

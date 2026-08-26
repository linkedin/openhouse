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

// The merge-on-read interaction family. A table created with the default copy-on-write delete mode
// is switched to merge-on-read partway through its life, so the case composes the mode change with
// the mutations that run after it.
trait MorInteractionScenarios extends MorScenarioKit {
  import Rows._

  def interactionMorCases(format: String): List[Plan.Case] = {
    val oneFilePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .sql("seed")(table =>
          s"INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM " +
            s"(${RowGenerator.valuesClause(Core, 3)}) AS seed")(),
      description = s"Three seed rows written as one data file in a $format table.")

    List(
      oneFilePreparation.test(
        "interact.mor.alterToMor",
        "Switching a table's delete mode to merge-on-read partway through its life makes a " +
          "subsequent partial-file DELETE write a position-delete file while preserving the " +
          "untouched rows in the data file.") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} SET TBLPROPERTIES " +
            "('write.delete.mode'='merge-on-read')")
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
        val deleteFileCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}.all_delete_files")
          .collect()(0)
          .getLong(0)
        val rowCount = table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0)

        assert(
          deleteFileCount == 1,
          s"ALTER-to-MoR should create one delete file, got $deleteFileCount")
        assert(
          rowCount == 2,
          s"ALTER-to-MoR delete should leave 2 rows, got $rowCount")
      })
  }
}

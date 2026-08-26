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

// The merge-on-read surface families. Both cases start from a table whose delete mode is
// merge-on-read and whose seed is one data file, so a strict-subset DELETE leaves a live
// position-delete file. One compacts that file through rewrite_position_delete_files, the other
// reads it back through the position_deletes metadata table. The cases run on parquet and orc.
trait MorSurfaceScenarios extends MorScenarioKit {
  import Rows._

  private def surfaceMergeOnReadPreparation(format: String): TablePreparation[CoreTable.type] =
    TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            "TBLPROPERTIES (" +
            s"'write.format.default'='$format', " +
            "'write.delete.mode'='merge-on-read')")()
        .sql("seed")(table =>
          s"INSERT INTO $table SELECT /*+ COALESCE(1) */ * FROM " +
            s"(${RowGenerator.valuesClause(Core, 3)}) AS seed")(),
      description = s"Three seed rows written as one data file in a merge-on-read $format " +
        "table.")

  // The rewrite procedure that compacts position-delete files.
  def morSurfaceRewriteProcedureCases(format: String): List[Plan.Case] =
    List(
      surfaceMergeOnReadPreparation(format).test(
        "surface.proc.rewritePositionDeletes",
        "After a MoR DELETE creates one position-delete file, rewrite_position_delete_files " +
          "compacts it while the 2 surviving rows remain readable.") { table =>
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.all_delete_files") == "1",
          "MoR delete should create one position-delete file")

        table.spark.sql(
          "CALL openhouse.system.rewrite_position_delete_files(" +
            s"table => '${catalogRelative(table.name)}', " +
            "options => map('rewrite-all', 'true'))")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}") == "2",
          "rewrite_position_delete_files should preserve live rows")
      })

  // The position_deletes metadata table.
  def morSurfaceMetadataCases(format: String): List[Plan.Case] =
    List(
      surfaceMergeOnReadPreparation(format).test(
        "surface.meta.positionDeletes",
        "After a MoR DELETE, the position_deletes metadata table reports exactly the one " +
          "position-delete entry it created.") { table =>
        table.spark.sql(
          s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 1")
        assert(
          countOf(
            table.spark,
            s"SELECT count(*) FROM ${table.name}.position_deletes") == "1",
          "position_deletes should expose the MoR position delete")
      })
}

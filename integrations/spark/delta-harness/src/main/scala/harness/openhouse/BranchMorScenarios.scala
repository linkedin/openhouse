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

// Branch merges on a merge-on-read table. A DELETE or UPDATE on a branch of a merge-on-read table
// writes position-delete files on the branch, and this family pins what merging that branch back to
// main does with them. It needs both the merge-on-read write modes and branch refs, so it belongs to
// the branch layer that sits above merge-on-read.
trait BranchMorScenarios extends BranchScenarioKit {
  import Rows._

  // Merge-on-read tables with branch merges: a DELETE or UPDATE on a branch of a MoR table writes
  // position-delete files on the branch, and merging the branch back to main must carry those
  // deletes correctly. The base table is a single-file MoR seed (a coalesced write of 1 file) so a
  // strict-subset DELETE produces a real position delete. Merge operates on refs and snapshots, so
  // one MoR layout covers the format-independent behavior. Each case checks that deletes are
  // carried across the merge, that deleted rows stay absent from main, and how cherry-pick handles
  // row-delete snapshots.
  lazy val morBranchMergeCases: List[Plan.Case] =
    morVerifyLayouts
      .filter(layout =>
        layout.label == "mor-verify/parquet" ||
          layout.label == "mor-verify/orc")
      .map(layout =>
        TablePreparation(
          layout.label,
          createAndSeedSingleFile(layout, 3),
          description = s"Three seed rows written as one data file in ${layout.description}."))
      .flatMap { preparation =>
        List(
          preparation.test(
            "mbranch.fastForwardDelete",
            "fast_forward carries a branch's position-delete DELETE onto main: main gains the " +
              "branch's 2-row state and the deleted row does not reappear.") { table =>
            table.spark.sql(
              s"ALTER TABLE ${table.name} CREATE BRANCH mfb")
            table.spark.sql(
              s"DELETE FROM ${table.name}.branch_mfb " +
                s"WHERE ${Core.long0.columnName} = 1")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name}") == "3",
              "main advanced before fast-forward")
            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mfb'") == "2",
              "branch delete was not applied")

            table.spark.sql(
              "CALL openhouse.system.fast_forward(" +
                s"'${catalogRelative(table.name)}', 'main', 'mfb')")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name}") == "2",
              "fast-forward did not carry the branch position delete")
            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name} " +
                  s"WHERE ${Core.long0.columnName} = 1") == "0",
              "deleted row reappeared after fast-forward")
          },
          preparation.test(
            "mbranch.fastForwardUpdate",
            "fast_forward carries a branch's UPDATE onto main: the row count stays at 3 and main " +
              "reads the branch's updated value.") { table =>
            table.spark.sql(
              s"ALTER TABLE ${table.name} CREATE BRANCH mub")
            table.spark.sql(
              s"UPDATE ${table.name}.branch_mub " +
                s"SET ${Core.string0.columnName} = 'br-upd' " +
                s"WHERE ${Core.long0.columnName} = 2")
            table.spark.sql(
              "CALL openhouse.system.fast_forward(" +
                s"'${catalogRelative(table.name)}', 'main', 'mub')")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name}") == "3",
              "fast-forward of an update changed the main row count")
            assert(
              table.spark
                .sql(
                  s"SELECT ${Core.string0.columnName} FROM ${table.name} " +
                    s"WHERE ${Core.long0.columnName} = 2")
                .collect()(0)
                .getString(0) == "br-upd",
              "fast-forward did not carry the branch update")
          },
          preparation.test(
            "mbranch.cherrypickDelete",
            "Cherry-picking a branch's position-delete DELETE snapshot onto main applies that " +
              "delete to main, leaving 2 rows.") { table =>
            table.spark.sql(
              s"ALTER TABLE ${table.name} CREATE BRANCH mcb")
            table.spark.sql(
              s"DELETE FROM ${table.name}.branch_mcb " +
                s"WHERE ${Core.long0.columnName} = 1")
            val deleteSnapshotId = table.spark
              .sql(
                s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                  "ORDER BY committed_at DESC LIMIT 1")
              .collect()(0)
              .getLong(0)

            table.spark.sql(
              "CALL openhouse.system.cherrypick_snapshot(" +
                s"'${catalogRelative(table.name)}', ${deleteSnapshotId}L)")
            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name}") == "2",
              "cherry-pick should apply the branch delete to main")
          },
          preparation.test(
            "mbranch.replaceBranchDelete",
            "REPLACE BRANCH AS OF a pre-delete snapshot undoes a branch's earlier position-delete " +
              "DELETE, restoring the branch to 3 rows.") { table =>
            val seedSnapshotId = table.spark
              .sql(
                s"SELECT snapshot_id FROM ${table.name}.snapshots " +
                  "ORDER BY committed_at DESC LIMIT 1")
              .collect()(0)
              .getLong(0)

            table.spark.sql(
              s"ALTER TABLE ${table.name} CREATE BRANCH mrb")
            table.spark.sql(
              s"DELETE FROM ${table.name}.branch_mrb " +
                s"WHERE ${Core.long0.columnName} = 1")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mrb'") == "2",
              "branch delete was not applied")

            table.spark.sql(
              s"ALTER TABLE ${table.name} REPLACE BRANCH mrb " +
                s"AS OF VERSION $seedSnapshotId")

            assert(
              countOf(
                table.spark,
                s"SELECT count(*) FROM ${table.name} VERSION AS OF 'mrb'") == "3",
              "replacing the branch target did not undo its position delete")
          })
      }
}

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

// The standard interaction families. Each case composes two table operations, so it shows how a
// DDL change, a snapshot reference, a maintenance procedure and a property setting behave against
// each other on a plain copy-on-write table. The cases run on parquet and orc.
trait InteractionScenarios extends ScenarioKit {
  import Rows._

  /**
   * After ADD COLUMN and an insert into the new column, time travel to the pre-DDL snapshot reads
   * the old schema with 3 rows, while a current read sees the new column.
   */
  private def interactDdlTtAfterAddColumnCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("interact.ddl.ttAfterAddColumn") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).last
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert9")
      val currentColumns = table.spark
        .sql(s"SELECT * FROM ${table.name} LIMIT 1")
        .columns
        .toSeq
      val historicalColumns = table.spark
        .sql(
          s"SELECT * FROM ${table.name} " +
            s"VERSION AS OF $seedSnapshotId LIMIT 1")
        .columns
        .toSeq
      val historicalRowCount = table.spark
        .sql(
          s"SELECT count(*) FROM ${table.name} " +
            s"VERSION AS OF $seedSnapshotId")
        .collect()(0)
        .getLong(0)

      assert(
        currentColumns.contains("extra_col"),
        s"current read is missing the evolved column: $currentColumns")
      assert(
        !historicalColumns.contains("extra_col") &&
          historicalColumns.size == Core.tableColumns.size,
        s"time travel should use the snapshot schema: $historicalColumns")
      assert(
        historicalRowCount == 3,
        s"pre-DDL snapshot should contain 3 rows, got $historicalRowCount")
    }

  /**
   * Rolling back to the pre-DDL snapshot after ADD COLUMN and an insert keeps the evolved schema,
   * restores 3 rows reading null for the new column, and the table still accepts writes into that
   * column.
   */
  private def interactDdlRestoreAfterAddColumnCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("interact.ddl.restoreAfterAddColumn") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).last
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert9")
      table.spark.sql(
        "CALL openhouse.system.rollback_to_snapshot(" +
          s"'${catalogRelative(table.name)}', $seedSnapshotId)")
      val currentColumns = table.spark
        .sql(s"SELECT * FROM ${table.name} LIMIT 1")
        .columns
        .toSeq
      val currentRowCount = table.spark
        .sql(s"SELECT count(*) FROM ${table.name}")
        .collect()(0)
        .getLong(0)
      val nonNullEvolvedValueCount = table.spark
        .sql(
          s"SELECT count(*) FROM ${table.name} " +
            "WHERE extra_col IS NOT NULL")
        .collect()(0)
        .getLong(0)

      assert(
        currentColumns.contains("extra_col"),
        s"rollback should retain the evolved schema: $currentColumns")
      assert(
        currentRowCount == 3,
        s"rollback should restore 3 rows, got $currentRowCount")
      assert(
        nonNullEvolvedValueCount == 0,
        "rolled-back rows should read the evolved column as null")

      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert10")
      assert(
        table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0) == 4,
        "the rolled-back table should accept evolved-schema writes")
    }

  /**
   * DROP COLUMN on a column that holds data is rejected, the column's data remains readable, and
   * the table remains writable.
   */
  private def interactDdlDropColAfterDataCase(
      preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("interact.ddl.dropColAfterData") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert9")
      val exception = Check.intercept[BadRequestException](
        table.spark.sql(
          s"ALTER TABLE ${table.name} DROP COLUMN extra_col"))

      assert(
        exception.getMessage.contains("not found in newSchema"),
        s"drop rejection message changed: ${exception.getMessage.take(200)}")
      assert(
        table.spark
          .sql(
            s"SELECT count(*) FROM ${table.name} WHERE extra_col = 42")
          .collect()(0)
          .getLong(0) == 1,
        "rejected drop should leave the column data readable")

      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert10")
      assert(
        table.spark
          .sql(s"SELECT count(*) FROM ${table.name}")
          .collect()(0)
          .getLong(0) == 5,
        "rejected drop should leave the table writable")
    }

  /**
   * The DDL interactions. Every case starts from three seed rows in a table in the given file
   * format.
   */
  def interactionDdlCases(format: String): List[Plan.Case] = {
    val preparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())

    List(
      interactDdlTtAfterAddColumnCase(preparation),
      interactDdlRestoreAfterAddColumnCase(preparation),
      interactDdlDropColAfterDataCase(preparation))
  }

  /**
   * Compacting a table after an ADD COLUMN and inserts into the new column preserves all rows, the
   * new column's non-null values, and null for rows written before the column was added.
   */
  private def interactMaintCompactEvolvedCase(
      basePreparation: TablePreparation[CoreTable.type]): Plan.Case =
    basePreparation.test("interact.maint.compactEvolved") { table =>
      table.spark.sql(
        s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert9")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES $extraColInsert10")
      table.spark.sql(
        "CALL openhouse.system.rewrite_data_files(" +
          s"table => '${catalogRelative(table.name)}')")
      val rowCount = table.spark
        .sql(s"SELECT count(*) FROM ${table.name}")
        .collect()(0)
        .getLong(0)
      val evolvedValueCount = table.spark
        .sql(
          s"SELECT count(*) FROM ${table.name} " +
            "WHERE extra_col IN (42, 43)")
        .collect()(0)
        .getLong(0)
      val nullValueCount = table.spark
        .sql(
          s"SELECT count(*) FROM ${table.name} WHERE extra_col IS NULL")
        .collect()(0)
        .getLong(0)

      assert(
        rowCount == 5,
        s"compaction should preserve 5 rows, got $rowCount")
      assert(
        evolvedValueCount == 2,
        s"compaction should preserve two evolved values, got $evolvedValueCount")
      assert(
        nullValueCount == 3,
        s"pre-evolution rows should remain null, got $nullValueCount")
    }

  /**
   * The maintenance interactions. The case starts from three seed rows in a table in the given
   * file format.
   */
  def interactionMiscellaneousCases(
      format: String): List[Plan.Case] = {
    val basePreparation = TablePreparation(
      format,
      TableTest(Core)
        .sql("create")(table =>
          s"CREATE TABLE $table ($columnDefinitions) USING $dataSource " +
            s"TBLPROPERTIES ('write.format.default'='$format')")()
        .insert(3)())

    List(
      interactMaintCompactEvolvedCase(basePreparation))
  }
}

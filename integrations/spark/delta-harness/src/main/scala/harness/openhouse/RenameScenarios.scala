package harness

import com.linkedin.openhouse.javaclient.exception.WebClientResponseWithMessageException

/**
 * Table rename: ALTER TABLE RENAME TO moves a table to a new name with its rows, and the catalog refuses a rename onto
 * a name that is already taken.
 *
 * Operations: RENAME TO a free name followed by a read of both the new and the old name, then a rename back; and
 * RENAME TO the name of a table that already exists.
 *
 * Preparation axes: the standard seeded core table in each of the two columnar formats. The conflict family creates
 * and drops the table it collides with.
 *
 * Case families: two families contributing 4 cases.
 */
trait RenameScenarios extends ScenarioKit {

  /** Every rename case, one file format at a time. */
  lazy val renameCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        renameTableCase(preparedStandardTable(format)),
        renameTableConflictCase(preparedStandardTable(format), format))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * ALTER TABLE RENAME TO moves the table to the new name with its 3 rows intact, and the old name stops resolving. A
   * second rename puts the table back under its original name, which teardown drops. The rename boundary records the
   * live name after each accepted rename, so a failure between the two renames drops the table under the name it
   * currently answers to.
   */
  private def renameTableCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("rename.table") { table =>
      val renamedTable = s"${table.name}_ren"

      withTrackedRename(table.spark.sql(_), table.name) { renameTo =>
        renameTo(renamedTable)
        assert(
          countOf(table.spark, s"SELECT count(*) FROM $renamedTable") == "3",
          "the renamed table should keep its rows")
        Check.intercept[Exception](
          table.spark.sql(s"SELECT 1 FROM ${table.name} LIMIT 1"))
        renameTo(table.name)
      }
    }

  /** ALTER TABLE RENAME TO a name that already exists is rejected with an error naming the conflict. */
  private def renameTableConflictCase(
      preparation: TablePreparation[CoreTable.type],
      format: String): Plan.Case =
    preparation.test("rename.table.conflict") { table =>
      val conflictingTable = s"${table.name}_other"

      withOwnedTable(table.spark.sql(_), conflictingTable)(
        table.spark.sql(coreCreate(conflictingTable, format))) {
        val exception = Check.intercept[WebClientResponseWithMessageException](
          table.spark.sql(s"ALTER TABLE ${table.name} RENAME TO $conflictingTable"))

        assert(
          exception.getMessage.contains("already exists"),
          s"unexpected message: ${exception.getMessage.take(160)}")
      }
    }

}

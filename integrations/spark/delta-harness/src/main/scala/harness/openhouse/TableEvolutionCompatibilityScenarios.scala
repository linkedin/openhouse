package harness

/**
 * One alteration a table can carry into the follow-up operations: the case-ID prefix its preparations contribute, the
 * preparation step that applies it, and the ALTER TABLE statement that step runs.
 */
private[harness] final case class TableAlteration(
  casePrefix: String,
  stepLabel: String,
  statement: String => String
)

/**
 * Table evolution compatibility: after a table has been altered, the reads, writes, snapshot operations and
 * maintenance procedures that worked before the alteration still work.
 *
 * Operations: INSERT INTO after the alteration, row-level DELETE after the alteration, a VERSION AS OF read of the
 * pre-alteration snapshot, rollback_to_snapshot back to that snapshot, expire_snapshots down to the newest snapshot,
 * and rewrite_data_files over the files written across the alteration.
 *
 * Preparation axes: the four Parquet and ORC core layouts (each format crossed with unpartitioned and
 * date-partitioned), each seeded with the standard rows and then altered in one of four ways: ADD COLUMN cc int,
 * widening foo_col_int from int to bigint, WRITE ORDERED BY foo_col_long, or setting write.distribution-mode to
 * range. That is 16 preparations.
 *
 * Case families: six families over 16 preparations, contributing 96 cases.
 */
trait TableEvolutionCompatibilityScenarios extends ScenarioKit {

  /** Every follow-up operation on every altered preparation, one preparation at a time. */
  lazy val tableEvolutionCompatibilityCases: List[Plan.Case] =
    alteredTablePreparations.flatMap(preparation =>
      List(
        insertCase(preparation),
        deleteCase(preparation),
        timeTravelCase(preparation),
        rollbackCase(preparation),
        expireSnapshotsCase(preparation),
        rewriteDataFilesCase(preparation)))

  /**
   * One preparation per Parquet and ORC layout and per alteration: the table is created, seeded with the standard
   * rows, then altered. Each alteration carries its own step label and case-ID prefix, so a case ID names the
   * alteration it ran after. Plan walks this list so every family lands on one preparation before the next
   * preparation starts.
   */
  lazy val alteredTablePreparations: List[TablePreparation[CoreTable.type]] =
    parquetAndOrcLayouts.flatMap { layout =>
      alterations.map { alteration =>
        TablePreparation(
          layout.label,
          create(layout)
            .insert(standardSeedRowCount)()
            .sql(alteration.stepLabel)(alteration.statement)(),
          alteration.casePrefix)
      }
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** The four alterations the follow-up operations run after, in the order Plan walks them. */
  private val alterations: List[TableAlteration] =
    List(
      TableAlteration(
        "afterAddColumn:",
        "addColumn",
        table => s"ALTER TABLE $table ADD COLUMN cc int"),
      TableAlteration(
        "afterTypeWiden:",
        "widenIntColumnToBigint",
        table => s"ALTER TABLE $table ALTER COLUMN ${Core.int0.columnName} TYPE bigint"),
      TableAlteration(
        "afterWriteOrder:",
        "writeOrderedByLongKey",
        table => s"ALTER TABLE $table WRITE ORDERED BY ${Core.long0.columnName}"),
      TableAlteration(
        "afterDistributionMode:",
        "setRangeDistributionMode",
        table =>
          s"ALTER TABLE $table SET TBLPROPERTIES ('write.distribution-mode'='range')"))

  /** A plain INSERT still lands on the table after the alteration, taking it to four rows. */
  private def insertCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("insert") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
          s"WHERE ${Core.long0.columnName} = 1")

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "4",
        "table is not writable after the alteration")
    }

  /** A row-level DELETE still lands on the table after the alteration, taking it to two rows. */
  private def deleteCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("delete") { table =>
      table.spark.sql(
        s"DELETE FROM ${table.name} WHERE ${Core.long0.columnName} = 2")

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "2",
        "mutation failed after the alteration")
    }

  /** The seed snapshot from before the alteration is still readable through VERSION AS OF and returns its 3 rows. */
  private def timeTravelCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("timeTravel") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head

      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} VERSION AS OF $seedSnapshotId") == "3",
        "seed snapshot is not readable after the alteration")
    }

  /**
   * rollback_to_snapshot back to the seed snapshot undoes an INSERT made after the alteration and returns the table to
   * its three seed rows.
   */
  private def rollbackCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("rollback") { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head

      table.spark.sql(
        s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
          s"WHERE ${Core.long0.columnName} = 1")
      table.spark.sql(
        "CALL openhouse.system.rollback_to_snapshot(" +
          s"'${catalogRelative(table.name)}', $seedSnapshotId)")

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "3",
        "rollback across the alteration failed")
    }

  /** expire_snapshots retaining only the newest snapshot leaves the table readable with its four current rows. */
  private def expireSnapshotsCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("expireSnapshots") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
          s"WHERE ${Core.long0.columnName} = 1")
      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
          "retain_last => 1)")

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "4",
        "table is unreadable after snapshot expiration")
    }

  /** rewrite_data_files compacts the files written across the alteration and preserves the four current rows. */
  private def rewriteDataFilesCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("rewriteDataFiles") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} SELECT * FROM ${table.name} " +
          s"WHERE ${Core.long0.columnName} = 1")
      table.spark.sql(
        "CALL openhouse.system.rewrite_data_files(" +
          s"table => '${catalogRelative(table.name)}', " +
          "options => map('min-input-files', '2'))")

      assert(
        countOf(table.spark, s"SELECT count(*) FROM ${table.name}") == "4",
        "compaction changed rows after the alteration")
    }

}

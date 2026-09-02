package harness

/**
 * Time travel: reading a table as it stood at an earlier snapshot, by snapshot ID, by commit timestamp, and after the
 * schema has moved on.
 *
 * Operations: VERSION AS OF each snapshot ID, TIMESTAMP AS OF the first commit's timestamp, and a VERSION AS OF read
 * of the pre-evolution snapshot after ADD COLUMN and an insert into the new column.
 *
 * Preparation axes: in each of the two columnar formats, the two-snapshot core table for the snapshot and timestamp
 * families, and the standard seeded core table for the schema-evolution family.
 *
 * Case families: three families contributing 6 cases.
 */
trait ScenarioTimeTravel extends ScenarioKit {

  /** Every time-travel case, one file format at a time. */
  lazy val timeTravelCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      List(
        versionAsOfCase(preparedTwoSnapshotTable(format)),
        timestampAsOfCase(preparedTwoSnapshotTable(format)),
        afterAddColumnCase(preparedStandardTable(format)))
    }

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /**
   * VERSION AS OF the first snapshot ID reads the 3 rows the seed commit wrote, and VERSION AS OF the second reads all
   * 5 rows.
   */
  private def versionAsOfCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("timeTravel.versionAsOf") { table =>
      val snapshots = snapshotIds(table.spark, table.name)

      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} VERSION AS OF ${snapshots(0)}") == "3")
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} VERSION AS OF ${snapshots(1)}") == "5")
    }

  /** TIMESTAMP AS OF the first commit's time reads the 3 rows that commit wrote. */
  private def timestampAsOfCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("timeTravel.timestampAsOf") { table =>
      val firstCommitTimestamp = table.spark
        .sql(
          s"SELECT CAST(committed_at AS STRING) FROM ${table.name}.snapshots " +
            "ORDER BY committed_at LIMIT 1")
        .collect()(0)
        .getString(0)

      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} TIMESTAMP AS OF '$firstCommitTimestamp'") == "3")
    }

  /**
   * After ADD COLUMN and an insert into the new column, time travel to the pre-evolution snapshot reads the old schema
   * with 3 rows, while a current read sees the new column.
   */
  private def afterAddColumnCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("timeTravel.afterAddColumn") { table =>
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

      assert(
        currentColumns.contains("extra_col"),
        s"current read is missing the evolved column: $currentColumns")
      assert(
        !historicalColumns.contains("extra_col") &&
          historicalColumns.size == Core.tableColumns.size,
        s"time travel should use the snapshot schema: $historicalColumns")
      assert(
        countOf(
          table.spark,
          s"SELECT count(*) FROM ${table.name} VERSION AS OF $seedSnapshotId") == "3",
        "pre-evolution snapshot should contain 3 rows")
    }

}

package harness

import org.apache.spark.sql.SparkSession

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
trait ScenarioTimeTravel extends HistoryTableFixtures {

  /** The standard seed as its (key, string) rows, so a snapshot read proves it returned exactly these rows. */
  private val seedKeyStrings: Seq[(Long, String)] =
    Seq((1L, "row-1"), (2L, "row-2"), (3L, "row-3"))

  /** The two-snapshot table's second commit as its (key, string) rows: the seed plus rows 4 and 5. */
  private val secondSnapshotKeyStrings: Seq[(Long, String)] =
    seedKeyStrings ++ Seq((4L, "row-4"), (5L, "row-5"))

  /** Every time-travel case, one file format at a time. */
  lazy val timeTravelCases: List[TestCase] =
    fileFormats.flatMap { format =>
      List(
        versionAsOfCase(preparedTwoSnapshotTable(format)),
        timestampAsOfCase(preparedTwoSnapshotTable(format)),
        afterAddColumnCase(preparedStandardTable(format)))
    }

  // --- the shared helpers and case bodies the surface above composes ---

  /** The (key, string) rows `source` returns, in ascending key order. `source` may carry a VERSION/TIMESTAMP clause. */
  private def keyStringsFrom(spark: SparkSession, source: String): Seq[(Long, String)] =
    spark
      .sql(
        s"SELECT ${Core.long0.columnName}, ${Core.string0.columnName} FROM $source " +
          s"ORDER BY ${Core.long0.columnName}")
      .collect()
      .toSeq
      .map(row => (row.getLong(0), row.getString(1)))

  /** The column names `source` exposes, in declaration order. `source` may carry a VERSION/TIMESTAMP clause. */
  private def columnNamesFrom(spark: SparkSession, source: String): Seq[String] =
    spark.sql(s"SELECT * FROM $source LIMIT 1").columns.toSeq

  /** VERSION AS OF the first snapshot reads exactly the seed rows, and the second reads the seed plus rows 4 and 5. */
  private def versionAsOfCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("timeTravel.versionAsOf") { table =>
      val history = snapshotIds(table.spark, table.name)
      assert(history.size == 2, s"the two-snapshot table has two commits, found ${history.size}")

      assert(
        keyStringsFrom(table.spark, s"${table.name} VERSION AS OF ${history(0)}") == seedKeyStrings,
        "the first snapshot reads exactly the seed rows")
      assert(
        keyStringsFrom(table.spark, s"${table.name} VERSION AS OF ${history(1)}") ==
          secondSnapshotKeyStrings,
        "the second snapshot reads the seed plus rows 4 and 5")
    }

  /** TIMESTAMP AS OF the first commit's distinct timestamp reads exactly the seed rows. */
  private def timestampAsOfCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("timeTravel.timestampAsOf") { table =>
      val commitTimes = table.spark
        .sql(s"SELECT committed_at FROM ${table.name}.snapshots ORDER BY committed_at")
        .collect()
        .toSeq
        .map(row => row.getTimestamp(0).getTime)
      assert(
        commitTimes.size == 2 && commitTimes.distinct.size == 2,
        s"the two commits carry distinct timestamps: $commitTimes")
      val firstCommitTimestamp = table.spark
        .sql(
          s"SELECT CAST(committed_at AS STRING) FROM ${table.name}.snapshots " +
            "ORDER BY committed_at LIMIT 1")
        .collect()(0)
        .getString(0)

      assert(
        keyStringsFrom(table.spark, s"${table.name} TIMESTAMP AS OF '$firstCommitTimestamp'") ==
          seedKeyStrings,
        "the first commit's timestamp reads exactly the seed rows")
    }

  /**
   * After ADD COLUMN and an insert into the new column, the current read carries the evolved schema, while time travel
   * to the pre-evolution snapshot reads the seed schema and exactly the seed rows.
   */
  private def afterAddColumnCase(preparation: TablePreparation[CoreTable.type]): TestCase =
    preparation.test("timeTravel.afterAddColumn") { table =>
      val seedSnapshot = snapshotIds(table.spark, table.name).last
      table.spark.sql(s"ALTER TABLE ${table.name} ADD COLUMN extra_col INT")
      table.spark.sql(s"INSERT INTO ${table.name} VALUES $extraColumnRowNine")

      assert(
        columnNamesFrom(table.spark, table.name) == Core.columnNames :+ "extra_col",
        s"the current read carries the evolved schema: ${columnNamesFrom(table.spark, table.name)}")
      assert(
        columnNamesFrom(table.spark, s"${table.name} VERSION AS OF $seedSnapshot") ==
          Core.columnNames,
        "time travel reads the pre-evolution schema")
      assert(
        keyStringsFrom(table.spark, s"${table.name} VERSION AS OF $seedSnapshot") == seedKeyStrings,
        "time travel reads exactly the seed rows")
    }

}

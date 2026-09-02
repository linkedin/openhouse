package harness

/**
 * One changelog operation: the name its case carries, the statement it runs against the prepared table, and the
 * change-type histogram the changelog view reports for the snapshot range that statement opened.
 */
final case class ChangelogOperation(
  name: String,
  statement: String => String,
  expectedChangeCounts: Map[String, Long]
)

/**
 * Changelog: the row-level change feed `create_changelog_view` reports for a snapshot range, and what it reports once
 * the start of that range has been expired.
 *
 * Operations: five reusable changelog operations (an append, an INSERT OVERWRITE that drops one row, a row-level
 * DELETE, an UPDATE, and a MERGE that updates one row and inserts another), each followed by a changelog view opened
 * at the seed snapshot; a changelog view over an append-only history with no start snapshot; and a changelog view
 * opened at three start points inside an expired snapshot range.
 *
 * Preparation axes: in each of the two columnar formats, the standard seeded core table for the five operations and
 * for the expired-range family, and the two-snapshot core table for the append-only history family. The operations are
 * data, so a feature layer covers its own table mode by crossing `changelogOperations` with its own preparations.
 *
 * Case families: three families contributing 14 cases, 10 operation cases, 2 append-only history cases and 2
 * expired-range cases.
 */
trait ScenarioChangelog extends ScenarioKit {

  /** Every changelog case, one file format at a time. */
  lazy val changelogCases: List[Plan.Case] =
    standardFormats.flatMap { format =>
      changelogOperationCasesFor(List(preparedStandardTable(format))) ++
        List(
          appendOnlyHistoryCase(preparedTwoSnapshotTable(format)),
          expiredRangeCase(preparedStandardTable(format)))
    }

  /**
   * The five row-level operations whose change feed the catalog reports. Every one starts from the standard three-row
   * seed, so its expected histogram holds on any preparation that seeds those rows.
   */
  lazy val changelogOperations: List[ChangelogOperation] =
    List(
      ChangelogOperation(
        "changelog.append",
        table =>
          s"INSERT INTO $table VALUES " +
            "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')",
        Map("INSERT" -> 1L)),
      ChangelogOperation(
        "changelog.overwrite",
        table =>
          s"INSERT OVERWRITE $table SELECT * FROM $table " +
            s"WHERE ${Core.long0.columnName} <= 2",
        Map("DELETE" -> 1L)),
      ChangelogOperation(
        "changelog.delete",
        table => s"DELETE FROM $table WHERE ${Core.long0.columnName} = 1",
        Map("DELETE" -> 1L)),
      ChangelogOperation(
        "changelog.update",
        table =>
          s"UPDATE $table SET ${Core.string0.columnName} = 'upd' " +
            s"WHERE ${Core.long0.columnName} = 2",
        Map("DELETE" -> 1L, "INSERT" -> 1L)),
      ChangelogOperation(
        "changelog.merge",
        table =>
          s"MERGE INTO $table target " +
            "USING (SELECT CAST(2 AS BIGINT) key " +
            "UNION ALL SELECT CAST(9 AS BIGINT)) source " +
            s"ON target.${Core.long0.columnName} = source.key " +
            s"WHEN MATCHED THEN UPDATE SET ${Core.string0.columnName} = 'm' " +
            "WHEN NOT MATCHED THEN INSERT " +
            s"(${Core.long0.columnName}, ${Core.int0.columnName}, " +
            s"${Core.string0.columnName}, ${Core.double0.columnName}, " +
            s"${Core.boolean0.columnName}, ${Core.date0.columnName}) " +
            "VALUES (source.key, 9, 'row-9', 9.5, true, '2024-01-09-01')",
        Map("DELETE" -> 1L, "INSERT" -> 2L)))

  /** The changelog cases for every operation on every preparation given, one preparation at a time. */
  def changelogOperationCasesFor(
      preparations: List[TablePreparation[CoreTable.type]]
  ): List[Plan.Case] =
    preparations.flatMap(preparation =>
      changelogOperations.map(operation => changelogOperationCase(preparation, operation)))

  // --- the preparations, shared helpers and case bodies the surface above composes ---

  /** The change-type histogram the named changelog view reports. */
  private def changeCounts(table: PreparedTable[CoreTable.type], view: String): Map[String, Long] =
    table.spark
      .sql(s"SELECT _change_type, count(*) FROM $view GROUP BY _change_type")
      .collect()
      .map(row => row.getString(0) -> row.getLong(1))
      .toMap

  /**
   * Running the operation against a seeded table and opening a changelog view at the seed snapshot reports exactly the
   * change types and counts that operation is defined to produce.
   */
  private def changelogOperationCase(
      preparation: TablePreparation[CoreTable.type],
      operation: ChangelogOperation): Plan.Case =
    preparation.test(operation.name) { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(operation.statement(table.name))
      val view = table.spark
        .sql(
          "CALL openhouse.system.create_changelog_view(" +
            s"table => '${catalogRelative(table.name)}', " +
            s"options => map('start-snapshot-id', '$seedSnapshotId'))")
        .collect()(0)
        .getString(0)

      val actualChangeCounts = changeCounts(table, view)

      assert(
        actualChangeCounts == operation.expectedChangeCounts,
        s"${operation.name} reported $actualChangeCounts, expected ${operation.expectedChangeCounts}")
    }

  /** create_changelog_view over an append-only history reports 5 changes, all of change type INSERT. */
  private def appendOnlyHistoryCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("changelog.appendOnlyHistory") { table =>
      val view = table.spark
        .sql(
          "CALL openhouse.system.create_changelog_view(" +
            s"table => '${catalogRelative(table.name)}')")
        .collect()(0)
        .getString(0)
      val actualChangeCounts = changeCounts(table, view)

      assert(
        actualChangeCounts == Map("INSERT" -> 5L),
        s"append-only changelog should report five inserts: $actualChangeCounts")
    }

  /**
   * After expire_snapshots removes a changelog start point, create_changelog_view over that start point either throws
   * or reports fewer changes than the table's history holds, and any message it throws leaves expiration unnamed. The
   * case covers three start points: an expired snapshot ID, a timestamp older than the whole history, and a timestamp
   * inside the expired range.
   */
  private def expiredRangeCase(preparation: TablePreparation[CoreTable.type]): Plan.Case =
    preparation.test("changelog.expiredRange") { table =>
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(6 AS BIGINT), 6, 'row-6', 6.5, true, '2024-01-06-05')")
      table.spark.sql(
        s"INSERT INTO ${table.name} VALUES " +
          "(CAST(7 AS BIGINT), 7, 'row-7', 7.5, true, '2024-01-07-06')")
      val snapshots = snapshotIds(table.spark, table.name)
      val firstTimestamp = table.spark
        .sql(
          s"SELECT committed_at FROM ${table.name}.snapshots " +
            "ORDER BY committed_at LIMIT 1")
        .collect()(0)
        .getTimestamp(0)
      val middleTimestamp = table.spark
        .sql(
          s"SELECT committed_at FROM ${table.name}.snapshots " +
            s"WHERE snapshot_id = ${snapshots(1)}")
        .collect()(0)
        .getTimestamp(0)
      table.spark.sql(
        "CALL openhouse.system.expire_snapshots(" +
          s"table => '${catalogRelative(table.name)}', " +
          "older_than => TIMESTAMP '2999-01-01 00:00:00', " +
          "retain_last => 1)")

      def changelogOutcome(
          optionKey: String,
          optionValue: String,
          trueChangeCount: Long): String =
        try {
          val view = table.spark
            .sql(
              "CALL openhouse.system.create_changelog_view(" +
                s"table => '${catalogRelative(table.name)}', " +
                s"options => map('$optionKey', '$optionValue'))")
            .collect()(0)
            .getString(0)
          val actualChangeCount = table.spark
            .sql(s"SELECT count(*) FROM $view")
            .collect()(0)
            .getLong(0)
          if (actualChangeCount < trueChangeCount) {
            s"SILENT under-report: $actualChangeCount of $trueChangeCount true changes"
          } else {
            s"FULL: $actualChangeCount of $trueChangeCount"
          }
        } catch {
          case exception: Throwable =>
            s"TYPED: ${exception.getClass.getSimpleName} :: " +
              Option(exception.getMessage).getOrElse("").take(140)
        }

      val outcomes = List(
        "explicitExpiredId" -> changelogOutcome("start-snapshot-id", snapshots.head.toString, 5),
        "timestampBeforeHistory" ->
          changelogOutcome("start-timestamp", (firstTimestamp.getTime - 1000).toString, 5),
        "timestampInsideExpiredRange" ->
          changelogOutcome("start-timestamp", (middleTimestamp.getTime - 1).toString, 2))

      outcomes.foreach { case (startPoint, outcome) =>
        println(s"DIAG changelog.expiredRange $startPoint: $outcome")
        assert(
          !outcome.startsWith("FULL"),
          s"expired-lineage changelog returned full truth for $startPoint")
        assert(
          !outcome.toLowerCase.contains("expir"),
          s"expired-lineage message now names expiration for $startPoint")
      }
    }

}

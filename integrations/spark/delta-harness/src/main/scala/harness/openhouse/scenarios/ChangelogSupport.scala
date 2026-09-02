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
 * Reusable changelog support for capability layers. It contributes zero catalog cases while holding the row-level
 * operations whose change feed `create_changelog_view` reports and the factory that turns those operations into cases
 * on preparations a caller supplies.
 *
 * A feature layer that needs changelog signal mixes this trait in and crosses `changelogOperations` with its own
 * preparations. The replace-table layer uses it to require rejection when a changelog range crosses a table
 * replacement. The follow-up standard changelog scenario builds on the same operation definitions.
 */
trait ChangelogSupport extends ScenarioKit {

  /**
   * The five row-level operations whose change feed the catalog reports: an append, an INSERT OVERWRITE that drops one
   * row, a row-level DELETE, an UPDATE, and a MERGE that updates one row and inserts another. Every one starts from
   * the standard three-row seed, so its expected histogram holds on any preparation that seeds those rows.
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
  ): List[TestCase] =
    preparations.flatMap(preparation =>
      changelogOperations.map(operation => changelogOperationCase(preparation, operation)))

  /** The change-type histogram the named changelog view reports. */
  def changeCounts(table: PreparedTable[CoreTable.type], view: String): Map[String, Long] =
    table.spark
      .sql(s"SELECT _change_type, count(*) FROM $view GROUP BY _change_type")
      .collect()
      .map(row => row.getString(0) -> row.getLong(1))
      .toMap

  /** The name of a changelog view over `table`, opened at `startSnapshotId`. */
  def changelogViewFrom(
      table: PreparedTable[CoreTable.type],
      startSnapshotId: Long): String =
    table.spark
      .sql(
        "CALL openhouse.system.create_changelog_view(" +
          s"table => '${catalogRelative(table.name)}', " +
          s"options => map('start-snapshot-id', '$startSnapshotId'))")
      .collect()(0)
      .getString(0)

  // --- the case body the surface above composes ---

  /**
   * Running the operation against a seeded table and opening a changelog view at the seed snapshot reports exactly the
   * change types and counts that operation is defined to produce.
   */
  private def changelogOperationCase(
      preparation: TablePreparation[CoreTable.type],
      operation: ChangelogOperation): TestCase =
    preparation.test(operation.name) { table =>
      val seedSnapshotId = snapshotIds(table.spark, table.name).head
      table.spark.sql(operation.statement(table.name))
      val actualChangeCounts = changeCounts(table, changelogViewFrom(table, seedSnapshotId))

      assert(
        actualChangeCounts == operation.expectedChangeCounts,
        s"${operation.name} reported $actualChangeCounts, expected ${operation.expectedChangeCounts}")
    }

}

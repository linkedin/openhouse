package harness

/**
 * Layout and side-table fixtures used by compatibility, locking, and streaming scenarios.
 */
trait CompatibilityTableFixtures extends RtasTableFixtures {

  /** Unpartitioned and date-partitioned core layouts in each file format. */
  lazy val compatibilityCoreLayouts: List[Layout] =
    fileFormats.flatMap(format =>
      List(
        coreLayout(s"unpartitioned/$format", format, ""),
        coreLayout(
          s"partitioned/$format",
          format,
          s"PARTITIONED BY (${Core.date0.columnName})")))

  /** The CREATE statement for an unpartitioned core table in `format`. */
  protected def coreCreateStatement(table: String, format: String): String =
    coreLayout(format, format, "").create(table)

  /** Runs a side-table operation and drops the table after the operation completes. */
  protected def withOwnedTable(runStatement: String => Unit, table: String)(
      create: => Unit)(use: => Unit): Unit =
    OwnedTableLifecycle.withOwnership(runStatement(s"DROP TABLE IF EXISTS $table")) {
      markTableCreated =>
        create
        markTableCreated()
        use
    }
}

package harness

/**
 * Side-table ownership and metadata fixtures used by governance scenarios.
 */
trait GovernanceTableFixtures extends RtasTableFixtures {

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

package harness

// The branch and write-audit-publish preparation kit. A branch preparation seeds main, creates
// branch b, and routes the session at that branch through spark.wap.branch, so every read and write
// the case performs lands on the branch while main keeps its seed rows. This layer sits above
// merge-on-read, so it also owns the branch-on-merge-on-read preparations. The members are lazy so
// they initialize on first read, after every trait mixed into `object Scenarios` has been
// constructed.
trait BranchScenarioKit extends MorScenarioKit {

  // Seed on main, create a branch, then set spark.wap.branch so every later read and write in the
  // case lands on the branch. A case captures its own before state from the branch and asserts
  // against it, so the same case body holds on a branch and on main. Each case runs in its own
  // spark.newSession(), which keeps the setting scoped to that case.
  def createAndSeedOnBranch(layout: Layout, numberOfRows: Int): TableTest[CoreTable.type] =
    createAndSeed(layout, numberOfRows)
      .sql("prep.enableWap")(t => s"ALTER TABLE $t SET TBLPROPERTIES ('write.wap.enabled'='true')")()
      .step("prep.routeToBranch") { (spark, table) =>
        spark.sql(s"ALTER TABLE $table CREATE BRANCH b")
        spark.conf.set("spark.wap.branch", "b")
      }()

  private def assertBranchMainIsolation(table: PreparedTable[CoreTable.type]): Unit = {
    table.spark.conf.unset("spark.wap.branch")
    val mainCount = table.spark
      .sql(s"SELECT count(*) FROM ${table.name}")
      .collect()(0)
      .getLong(0)
    assert(
      mainCount == 3,
      s"branch operation leaked to main: expected 3 rows, got $mainCount")
  }

  private def branchPreparationDescription(layout: Layout): String =
    s"Three seed rows with keys 1, 2 and 3 in ${layout.description}, with write.wap.enabled set, " +
      "branch b created, and spark.wap.branch set to b, so every read and write in the case lands " +
      "on branch b while main keeps its three seed rows."

  lazy val preparedBranchCoreTables: List[TablePreparation[CoreTable.type]] =
    layouts.map { layout =>
      TablePreparation(
        layout.label,
        createAndSeedOnBranch(layout, 3),
        "branchWap:",
        assertBranchMainIsolation,
        branchPreparationDescription(layout))
    }

  lazy val preparedPartitionedBranchCoreTables: List[TablePreparation[CoreTable.type]] =
    partitionedLayouts.map { layout =>
      TablePreparation(
        layout.label,
        createAndSeedOnBranch(layout, 3),
        "branchWap:",
        assertBranchMainIsolation,
        branchPreparationDescription(layout))
    }

  lazy val preparedBranchMorCoreTables: List[TablePreparation[CoreTable.type]] =
    unpartitionedMorLayouts.map { layout =>
      TablePreparation(
        layout.label,
        createAndSeedOnBranch(layout, 3),
        "branchWap:",
        assertBranchMainIsolation,
        branchPreparationDescription(layout))
    }

  lazy val preparedNullStringBranchCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedBranchCoreTables.map(withNullStringRow)

  lazy val preparedNullStringBranchMorCoreTables: List[TablePreparation[CoreTable.type]] =
    preparedBranchMorCoreTables.map(withNullStringRow)

  lazy val branchLayoutFormatPreparations: List[TablePreparation[CoreTable.type]] =
    preparedBranchCoreTables

  def branchLayoutFormatCases: List[Plan.Case] =
    layoutFormatCasesFor(branchLayoutFormatPreparations)
}

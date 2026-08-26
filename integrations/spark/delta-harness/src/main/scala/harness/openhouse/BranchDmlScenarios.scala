package harness

// The branch DML buckets. Each bucket is a branch-routed preparation list crossed with one of the
// shared DML test-case lists that DmlScenarios names. A case captures its before state from the
// branch it is routed at, so the same body holds on a branch and on main, and the preparation's own
// isolation check confirms main kept its three seed rows.
trait BranchDmlScenarios extends BranchScenarioKit { this: DmlScenarios =>

  lazy val branchDmlCases: List[Plan.Case] =
    preparedBranchCoreTables.flatMap(preparation => allDmlTestCases.map(_.runOn(preparation))) ++
      preparedNullStringBranchCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  lazy val branchPartitionedDmlCases: List[Plan.Case] =
    preparedPartitionedBranchCoreTables.flatMap(preparation =>
      partitionedTableTestCases.map(_.runOn(preparation)))

  lazy val branchMorDmlCases: List[Plan.Case] =
    preparedBranchMorCoreTables.flatMap(preparation =>
      rowMutationTestCases.map(_.runOn(preparation))) ++
      preparedNullStringBranchMorCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  // The branch a consumer creates after the DDL. It runs against each of the standard DDL-consumer
  // preparations, so Plan places it inside that walk.
  def branchDdlConsumerCases(
      preparation: TablePreparation[CoreTable.type]): List[Plan.Case] =
    List(
      preparation.test(
        "branch",
        "A write to a branch created after the DDL takes the branch to four rows and leaves " +
          "main on its three rows.") { table =>
        table.spark.sql(
          s"ALTER TABLE ${table.name} CREATE BRANCH cb")
        table.spark.sql(
          s"INSERT INTO ${table.name}.branch_cb " +
            s"SELECT * FROM ${table.name} " +
            s"WHERE ${Core.long0.columnName} = 1")

        assert(
          table.spark
            .sql(
              s"SELECT count(*) FROM ${table.name} VERSION AS OF 'cb'")
            .collect()(0)
            .getLong(0) == 4,
          "branch write failed after DDL")
        assert(
          table.spark
            .sql(s"SELECT count(*) FROM ${table.name}")
            .collect()(0)
            .getLong(0) == 3,
          "branch write changed the main table")
      })
}

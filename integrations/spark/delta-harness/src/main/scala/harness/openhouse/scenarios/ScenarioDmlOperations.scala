package harness

/**
 * Extends the representative DML slice with every reusable read, delete, update, merge, insert, and overwrite.
 *
 * Operations: 48 additional operations over the six CoreTable columns (bigint, int, string, double, boolean, and a
 * string-encoded date). The complete DML catalog contains 54 operations: 2 reads, 14 deletes, 13 updates, 16 merges,
 * 6 inserts or overwrites, 1 null-string delete, and 2 partition-scoped overwrites.
 *
 * Preparation axes: in each columnar format, the general operations run on the unpartitioned standard table, the
 * null-string DELETE runs on that table extended with one null value, and the partition-scoped overwrites run on the
 * date-partitioned standard table.
 *
 * This contribution adds 96 cases beyond the 12 representative DML cases in the foundation catalog.
 */
trait ScenarioDmlOperations
    extends ScenarioDmlRead
    with ScenarioDmlDelete
    with ScenarioDmlUpdate
    with ScenarioDmlMerge
    with ScenarioDmlWrite
    with DmlTableFixtures {
  self: ScenarioCoreDml =>

  /** Every additional DML operation on its canonical preparation in each file format. */
  lazy val dmlOperationCases: List[TestCase] = {
    val foundationDmlCaseIds = coreDmlOperations.map(_.id).toSet
    preparedCoreFormats
      .zip(preparedPartitionedCoreTables)
      .flatMap { case (standardPreparation, partitionedPreparation) =>
        allDmlTestCases
          .filterNot(testCase => foundationDmlCaseIds.contains(testCase.id))
          .map(_.runOn(standardPreparation)) ++
          nullStringRowTestCases.map(_.runOn(withNullStringRow(standardPreparation))) ++
          partitionedTableTestCases.map(_.runOn(partitionedPreparation))
      }
  }

  /** Every DML case that runs on a table with the seed row shape. */
  lazy val allDmlTestCases: List[DmlTestCase[CoreTable.type]] =
    readTestCases ++
      deleteTestCases ++
      updateTestCases ++
      mergeTestCases ++
      insertAndOverwriteTestCases

  /** The row-mutating cases: every DELETE, UPDATE and MERGE. */
  lazy val rowMutationTestCases: List[DmlTestCase[CoreTable.type]] =
    deleteTestCases ++ updateTestCases ++ mergeTestCases

  /**
   * The cases that address columns by name and preserve their assertions when the preparation adds columns.
   */
  lazy val testCasesCompatibleWithAnAddedColumn: List[DmlTestCase[CoreTable.type]] =
    readTestCases ++ deleteTestCases ++ updateTestCases
}

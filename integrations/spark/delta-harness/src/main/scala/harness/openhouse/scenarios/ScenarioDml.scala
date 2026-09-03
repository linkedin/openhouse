package harness

/**
 * Data manipulation on the core table: the reads, deletes, updates, merges, inserts and overwrites the catalog
 * supports, and the row and snapshot change each one commits.
 *
 * Operations: 54 reusable DML operations over the six CoreTable columns (bigint, int, string, double, boolean, and a
 * string-encoded date), made up of 2 reads, 14 deletes, 13 updates, 16 merges, 6 inserts or overwrites, 1 null-string
 * delete, and 2 partition-scoped overwrites. Each operation covers a distinct SQL or DataFrame form, or a distinct
 * observable state change within its family.
 *
 * Preparation axes: in each columnar format, the 51 general operations run on the unpartitioned standard table, the
 * null-string DELETE runs on that table extended with one null value, and the two partition-scoped overwrites run on
 * the date-partitioned standard table.
 *
 * Case families: 54 operations over Parquet and ORC, contributing 108 cases.
 */
trait ScenarioDml
    extends ScenarioDmlRead
    with ScenarioDmlDelete
    with ScenarioDmlUpdate
    with ScenarioDmlMerge
    with ScenarioDmlWrite {

  /** Every reusable DML operation on its canonical preparation in each file format. */
  lazy val dmlCases: List[TestCase] =
    preparedCoreFormats
      .zip(preparedPartitionedCoreTables)
      .flatMap { case (standardPreparation, partitionedPreparation) =>
        allDmlTestCases.map(_.runOn(standardPreparation)) ++
          nullStringRowTestCases.map(_.runOn(withNullStringRow(standardPreparation))) ++
          partitionedTableTestCases.map(_.runOn(partitionedPreparation))
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
   * The cases that address columns by name and never write a whole seed-shaped row, so they run on a preparation whose
   * column list has grown beyond the seed rows.
   */
  lazy val testCasesCompatibleWithAnAddedColumn: List[DmlTestCase[CoreTable.type]] =
    readTestCases ++ deleteTestCases ++ updateTestCases
}

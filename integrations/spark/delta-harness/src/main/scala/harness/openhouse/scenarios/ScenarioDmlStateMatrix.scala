package harness

/**
 * Standard DML compatibility across additional table states.
 *
 * The canonical DML layer proves every operation once per file format. This layer attacks the separate independence
 * claim that partitioning, write ordering, and an added nullable column preserve the operations compatible with each
 * state. Each generated case reuses the operation's exact row and snapshot oracle.
 *
 * Preparation axes: the two date-partitioned tables, four write-ordered tables, and four tables carrying one added
 * nullable column.
 *
 * Case families: 104 partitioned cases, 208 write-ordered cases, and 116 evolved-schema cases, contributing 428
 * cases.
 */
trait ScenarioDmlStateMatrix extends DmlStateTableFixtures {
  this: ScenarioCoreDml with ScenarioDmlOperations =>

  /** Every additional standard-state crossing, ordered by partitioned, write-ordered, then evolved state. */
  lazy val dmlStateMatrixCases: List[TestCase] =
    partitionedDmlCases ++ orderedDmlCases ++ evolvedDmlCases

  /** General and null-sensitive operations on date-partitioned tables. */
  private lazy val partitionedDmlCases: List[TestCase] =
    preparedPartitionedCoreTables.flatMap(preparation =>
      allDmlTestCases.map(_.runOn(preparation))) ++
      preparedPartitionedCoreTables
        .map(withNullStringRow)
        .flatMap(preparation => nullStringRowTestCases.map(_.runOn(preparation)))

  /** Every general and null-sensitive operation on tables carrying a write order. */
  private lazy val orderedDmlCases: List[TestCase] =
    preparedOrderedCoreTables.flatMap(preparation =>
      orderedDmlTestCases.map(_.runOn(preparation))) ++
      preparedNullStringOrderedCoreTables.flatMap(preparation =>
        nullStringRowTestCases.map(_.runOn(preparation)))

  /** Name-addressed reads and mutations on tables carrying an added nullable column. */
  private lazy val evolvedDmlCases: List[TestCase] =
    preparedEvolvedCoreTables.flatMap(preparation =>
      testCasesCompatibleWithAnAddedColumn.map(_.runOn(preparation)))

  /**
   * The operation list for write-ordered tables, with the partition-predicate DELETE carrying its known product bug.
   */
  private lazy val orderedDmlTestCases: List[DmlTestCase[CoreTable.type]] =
    allDmlTestCases.map {
      case testCase if testCase.id == "delete.byPartitionPredicate" =>
        testCase.copy(knownBugReason = Some(
          "DELETE by partition predicate crashes in the Spark and Iceberg rewrite when the " +
            "table has a write order."))
      case testCase =>
        testCase
    }
}

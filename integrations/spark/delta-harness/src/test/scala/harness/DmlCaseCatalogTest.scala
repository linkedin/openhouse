package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the shape the standard DML tests are written in: one list of test cases, one list of
 * preparations, and a bucket that is the cross of the two. Each feature layer pins its own buckets
 * in its own test. Reading these lists does not execute a case or start Spark.
 */
final class DmlCaseCatalogTest {
  private val expectedReadTestCaseIds = List("read.projection", "read.filter")

  private val expectedDeleteTestCaseIds = List(
    "delete.byPredicate",
    "delete.byInList",
    "delete.byInSubquery",
    "delete.byNotInSubquery",
    "delete.byExistsSubquery",
    "delete.byNotExistsSubquery",
    "delete.byScalarSubquery",
    "delete.all",
    "delete.none",
    "delete.byPartitionPredicate",
    "delete.withAlias",
    "delete.whereFalse.noSnapshot",
    "delete.truncate",
    "delete.atSnapshot.rejected")

  private val expectedUpdateTestCaseIds = List(
    "update.byPredicate",
    "update.withoutCondition",
    "update.noMatch",
    "update.byInSubquery",
    "update.byNotInSubquery",
    "update.byExistsSubquery",
    "update.byNotExistsSubquery",
    "update.byScalarSubquery",
    "update.withAlias",
    "update.multipleColumns",
    "update.byExpression",
    "update.movePartition",
    "update.nullAssignment")

  private val expectedMergeTestCaseIds = List(
    "merge.insertNotMatched",
    "merge.updateMatched",
    "merge.deleteMatched",
    "merge.upsert",
    "merge.deleteNotMatchedBySource",
    "merge.conditionalUpdate",
    "merge.multipleMatchedClauses",
    "merge.conditionalInsert",
    "merge.allClauses",
    "merge.updateStar",
    "merge.insertExplicitColumns",
    "merge.sourceCTE",
    "merge.sourceSetOp",
    "merge.intoEmptyTarget",
    "merge.nullJoinKey",
    "merge.resolveByName")

  private val expectedDmlTestCaseIds = List(
    "read.projection",
    "read.filter",
    "delete.byPredicate",
    "delete.byInList",
    "delete.byInSubquery",
    "delete.byNotInSubquery",
    "delete.byExistsSubquery",
    "delete.byNotExistsSubquery",
    "delete.byScalarSubquery",
    "delete.all",
    "delete.none",
    "delete.byPartitionPredicate",
    "delete.withAlias",
    "delete.whereFalse.noSnapshot",
    "delete.truncate",
    "delete.atSnapshot.rejected",
    "update.byPredicate",
    "update.withoutCondition",
    "update.noMatch",
    "update.byInSubquery",
    "update.byNotInSubquery",
    "update.byExistsSubquery",
    "update.byNotExistsSubquery",
    "update.byScalarSubquery",
    "update.withAlias",
    "update.multipleColumns",
    "update.byExpression",
    "update.movePartition",
    "update.nullAssignment",
    "merge.insertNotMatched",
    "merge.updateMatched",
    "merge.deleteMatched",
    "merge.upsert",
    "merge.deleteNotMatchedBySource",
    "merge.conditionalUpdate",
    "merge.multipleMatchedClauses",
    "merge.conditionalInsert",
    "merge.allClauses",
    "merge.updateStar",
    "merge.insertExplicitColumns",
    "merge.sourceCTE",
    "merge.sourceSetOp",
    "merge.intoEmptyTarget",
    "merge.nullJoinKey",
    "merge.resolveByName",
    "insert.into",
    "insert.explicitColumns",
    "insert.intoSelect",
    "append.dataFrame",
    "insert.overwrite",
    "overwrite.dataFrame")

  @Test
  def everyDmlTestCaseIsListedOnceInOrder(): Unit = {
    val caseIds = Scenarios.allDmlTestCases.map(_.id)

    assertEquals(expectedDmlTestCaseIds, caseIds)
    assertEquals(caseIds.distinct.size, caseIds.size, s"duplicate DML case id in $caseIds")
  }

  @Test
  def eachCompatibilityListNamesTheOperationsItsStartingStateSupports(): Unit = {
    assertEquals(
      expectedDeleteTestCaseIds ++ expectedUpdateTestCaseIds ++ expectedMergeTestCaseIds,
      Scenarios.rowMutationTestCases.map(_.id))
    assertEquals(
      expectedReadTestCaseIds ++ expectedDeleteTestCaseIds ++ expectedUpdateTestCaseIds,
      Scenarios.testCasesCompatibleWithAnAddedColumn.map(_.id))
    assertEquals(expectedReadTestCaseIds, Scenarios.readTestCases.map(_.id))
    assertEquals(List("delete.byNullCondition"), Scenarios.nullStringRowTestCases.map(_.id))
  }

  @Test
  def orderedPreparationMarksItsKnownFailingMatrixCellExplicitly(): Unit = {
    assertEquals(
      List("delete.byPartitionPredicate"),
      Scenarios.orderedDmlTestCases.collect {
        case testCase if testCase.knownBugReason.nonEmpty => testCase.id
      })
  }

  @Test
  def theNullStringPreparationsExtendTheCorePreparations(): Unit = {
    assertEquals(
      Scenarios.preparedCoreTables.map(preparation => (preparation.casePrefix, preparation.label)),
      Scenarios.preparedNullStringCoreTables.map(preparation =>
        (preparation.casePrefix, preparation.label)))
    assertEquals(
      Scenarios.preparedCoreTables.map(_.preparation.steps.size + 1),
      Scenarios.preparedNullStringCoreTables.map(_.preparation.steps.size))
    assertEquals(
      List("prep.nullStringRow"),
      Scenarios.preparedNullStringCoreTables.head.preparation.steps.map(_.label).toList.takeRight(1))
  }

  @Test
  def everyDmlCaseIdNamesItsOperationAndItsPreparation(): Unit = {
    val describedBuckets = List(
      Scenarios.coreDmlCases,
      Scenarios.orderedDmlCases,
      Scenarios.evolvedDmlCases,
      Scenarios.partitionedDmlCases,
      Scenarios.layoutFormatCases).flatten
    val caseIds = describedBuckets.map(_.id)

    caseIds.foreach { caseId =>
      assertEquals(
        2,
        caseId.split(" @ ").length,
        s"$caseId must be an operation name, then ' @ ', then a preparation label")
    }
    assertEquals(caseIds.distinct.size, caseIds.size, "DML case IDs must be unique")
  }

  @Test
  def eachLayoutListCrossesItsFormatsWithItsPartitionings(): Unit = {
    assertEquals(
      List(
        "unpartitioned/parquet",
        "partitioned/parquet",
        "unpartitioned/orc",
        "partitioned/orc",
        "unpartitioned/avro",
        "partitioned/avro"),
      Scenarios.layouts.map(_.label))
    assertEquals(
      List("partitioned/parquet", "partitioned/orc", "partitioned/avro"),
      Scenarios.partitionedLayouts.map(_.label))
    assertEquals(
      List(
        "unpartitioned/parquet",
        "partitioned/parquet",
        "unpartitioned/orc",
        "partitioned/orc"),
      Scenarios.parquetAndOrcLayouts.map(_.label))
  }

  @Test
  def formatMaterializationIsNotADmlOperation(): Unit = {
    assertTrue(
      !Scenarios.allDmlTestCases.map(_.id).contains("format.materialization"),
      "format.materialization describes the preparation, not an operation run against it")
    assertEquals(
      caseIds(Scenarios.layoutFormatPreparations, "format.materialization"),
      Scenarios.layoutFormatCases.map(_.id))
  }

  @Test
  def eachBucketIsThePreparationListCrossedWithItsTestCaseList(): Unit = {
    val noNullStringPreparations = List.empty[TablePreparation[CoreTable.type]]
    val buckets = List(
      ("coreDmlCases", Scenarios.coreDmlCases, Scenarios.preparedCoreTables, Scenarios.allDmlTestCases, Scenarios.preparedNullStringCoreTables),
      ("orderedDmlCases", Scenarios.orderedDmlCases, Scenarios.preparedOrderedCoreTables, Scenarios.allDmlTestCases, Scenarios.preparedNullStringOrderedCoreTables),
      ("evolvedDmlCases", Scenarios.evolvedDmlCases, Scenarios.preparedEvolvedCoreTables, Scenarios.testCasesCompatibleWithAnAddedColumn, noNullStringPreparations),
      ("partitionedDmlCases", Scenarios.partitionedDmlCases, Scenarios.preparedPartitionedCoreTables, Scenarios.partitionedTableTestCases, noNullStringPreparations))

    buckets.foreach { case (bucketName, bucket, preparations, testCases, nullStringPreparations) =>
      val expectedIds =
        caseIds(preparations, testCases) ++
          caseIds(nullStringPreparations, Scenarios.nullStringRowTestCases)

      assertEquals(
        expectedIds,
        bucket.map(_.id),
        s"$bucketName is not its named preparations crossed with its named test cases")
    }
  }

  private def caseIds(
      preparations: List[TablePreparation[CoreTable.type]],
      testCases: List[DmlTestCase[CoreTable.type]]
  ): List[String] =
    preparations.flatMap(preparation =>
      testCases.map(testCase =>
        s"${preparation.casePrefix}${testCase.id} @ ${preparation.label}"))

  private def caseIds(
      preparations: List[TablePreparation[CoreTable.type]],
      testCaseId: String
  ): List[String] =
    preparations.map(preparation =>
      s"${preparation.casePrefix}$testCaseId @ ${preparation.label}")
}

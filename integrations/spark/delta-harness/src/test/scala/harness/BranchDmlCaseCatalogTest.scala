package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the shape of the branch DML buckets: each one is a branch-routed preparation list crossed
 * with a DML test-case list the standard layer names. Reading these lists does not execute a case
 * or start Spark.
 */
final class BranchDmlCaseCatalogTest {

  @Test
  def eachBucketIsThePreparationListCrossedWithItsTestCaseList(): Unit = {
    assertEquals(
      caseIds(Scenarios.preparedBranchCoreTables, Scenarios.allDmlTestCases) ++
        caseIds(Scenarios.preparedNullStringBranchCoreTables, Scenarios.nullStringRowTestCases),
      Scenarios.branchDmlCases.map(_.id),
      "branchDmlCases is not its named preparations crossed with its named test cases")
    assertEquals(
      caseIds(Scenarios.preparedPartitionedBranchCoreTables, Scenarios.partitionedTableTestCases),
      Scenarios.branchPartitionedDmlCases.map(_.id),
      "branchPartitionedDmlCases is not its named preparations crossed with its named test cases")
    assertEquals(
      caseIds(Scenarios.preparedBranchMorCoreTables, Scenarios.rowMutationTestCases) ++
        caseIds(Scenarios.preparedNullStringBranchMorCoreTables, Scenarios.nullStringRowTestCases),
      Scenarios.branchMorDmlCases.map(_.id),
      "branchMorDmlCases is not its named preparations crossed with its named test cases")
  }

  @Test
  def everyBranchPreparationDescribesTheRoutingItSetsUp(): Unit = {
    val describedPreparations =
      Scenarios.preparedBranchCoreTables ++
        Scenarios.preparedPartitionedBranchCoreTables ++
        Scenarios.preparedBranchMorCoreTables

    describedPreparations.foreach { preparation =>
      assertTrue(
        preparation.description.contains("spark.wap.branch"),
        s"${preparation.label} does not describe the branch routing it sets up")
    }
  }

  @Test
  def theLayoutFormatCasesRunOnTheBranchPreparations(): Unit =
    assertEquals(
      caseIds(Scenarios.branchLayoutFormatPreparations, "format.materialization"),
      Scenarios.branchLayoutFormatCases.map(_.id))

  @Test
  def everyBranchCaseCarriesItsOwnDescriptionAndItsPreparationDescription(): Unit = {
    val describedBuckets = List(
      Scenarios.branchDmlCases,
      Scenarios.branchPartitionedDmlCases,
      Scenarios.branchMorDmlCases,
      Scenarios.branchLayoutFormatCases).flatten

    describedBuckets.foreach { testCase =>
      assertTrue(
        testCase.description.trim.nonEmpty,
        s"${testCase.id} has no description of the operation it runs")
      assertTrue(
        testCase.preparationDescription.trim.nonEmpty,
        s"${testCase.id} has no description of the state it starts from")
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

package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the shape of the RTAS DML buckets: each one is a replace-lineage preparation list crossed
 * with a DML test-case list the standard layer names. Reading these lists does not execute a case
 * or start Spark.
 */
final class RtasDmlCaseCatalogTest {

  @Test
  def eachBucketIsThePreparationListCrossedWithItsTestCaseList(): Unit = {
    assertEquals(
      caseIds(Scenarios.preparedRtasCoreTables, Scenarios.allDmlTestCases) ++
        caseIds(Scenarios.preparedNullStringRtasCoreTables, Scenarios.nullStringRowTestCases),
      Scenarios.rtasDmlCases.map(_.id),
      "rtasDmlCases is not its named preparations crossed with its named test cases")
    assertEquals(
      caseIds(Scenarios.preparedRtasPartitionedCoreTables, Scenarios.partitionedTableTestCases),
      Scenarios.rtasPartitionedDmlCases.map(_.id),
      "rtasPartitionedDmlCases is not its named preparations crossed with its named test cases")
  }

  @Test
  def theReplaceLineagePreparationsDescribeTheReplaceTheyPerform(): Unit = {
    Scenarios.preparedRtasCoreTables.foreach { preparation =>
      assertTrue(
        preparation.description.contains("CREATE OR REPLACE TABLE AS SELECT"),
        s"${preparation.label} does not describe the replace it performs")
    }
  }

  @Test
  def theLayoutFormatCasesRunOnTheReplaceLineagePreparations(): Unit =
    assertEquals(
      caseIds(Scenarios.rtasLayoutFormatPreparations, "format.materialization"),
      Scenarios.rtasLayoutFormatCases.map(_.id))

  @Test
  def everyRtasCaseCarriesItsOwnDescriptionAndItsPreparationDescription(): Unit = {
    val describedBuckets = List(
      Scenarios.rtasDmlCases,
      Scenarios.rtasPartitionedDmlCases,
      Scenarios.rtasLayoutFormatCases).flatten

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

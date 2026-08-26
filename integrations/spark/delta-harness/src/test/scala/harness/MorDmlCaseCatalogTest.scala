package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

/**
 * Pins the shape of the merge-on-read DML buckets: each one is a merge-on-read preparation list
 * crossed with a DML test-case list the standard layer names, plus the pair of cases that assert
 * the physical difference between the two write modes. Reading these lists does not execute a case
 * or start Spark.
 */
final class MorDmlCaseCatalogTest {

  @Test
  def eachBucketIsThePreparationListCrossedWithItsTestCaseList(): Unit = {
    assertEquals(
      caseIds(Scenarios.preparedMorCoreTables, Scenarios.rowMutationTestCases) ++
        caseIds(Scenarios.preparedNullStringMorCoreTables, Scenarios.nullStringRowTestCases),
      Scenarios.morDmlCases.map(_.id),
      "morDmlCases is not its named preparations crossed with its named test cases")
    assertEquals(
      caseIds(Scenarios.preparedRtasMorCoreTables, Scenarios.rowMutationTestCases) ++
        caseIds(Scenarios.preparedNullStringRtasMorCoreTables, Scenarios.nullStringRowTestCases),
      Scenarios.rtasMorDmlCases.map(_.id),
      "rtasMorDmlCases is not its named preparations crossed with its named test cases")
    assertEquals(
      caseIds(Scenarios.preparedMorReadCoreTables, Scenarios.readTestCases),
      Scenarios.morReadDmlCases.map(_.id),
      "morReadDmlCases is not its named preparations crossed with its named test cases")
  }

  @Test
  def theDeleteFileModeBucketPairsOneMergeOnReadCaseWithOneCopyOnWriteCase(): Unit =
    assertEquals(
      Scenarios.morVerifyLayouts.map(layout => s"mor.writesDeleteFiles @ ${layout.label}") ++
        Scenarios.cowVerifyLayouts.map(layout => s"cow.writesNoDeleteFiles @ ${layout.label}"),
      Scenarios.deleteFileModeCases.map(_.id))

  @Test
  def theLayoutFormatCasesRunOnTheMergeOnReadReadPreparations(): Unit =
    assertEquals(
      caseIds(Scenarios.morReadLayoutFormatPreparations, "format.materialization"),
      Scenarios.morReadLayoutFormatCases.map(_.id))

  @Test
  def everyMergeOnReadLayoutDescribesTheTableItCreates(): Unit = {
    val describedLayouts =
      Scenarios.morLayouts ++
        Scenarios.unpartitionedMorLayouts ++
        Scenarios.morVerifyLayouts ++
        Scenarios.cowVerifyLayouts

    describedLayouts.foreach { layout =>
      assertTrue(
        layout.description.trim.nonEmpty,
        s"layout ${layout.label} has no description")
      assertTrue(
        layout.description != layout.label,
        s"layout ${layout.label} repeats its label; the description must explain the table")
    }
  }

  @Test
  def everyMergeOnReadCaseCarriesItsOwnDescriptionAndItsPreparationDescription(): Unit = {
    val describedBuckets = List(
      Scenarios.morDmlCases,
      Scenarios.rtasMorDmlCases,
      Scenarios.morReadDmlCases,
      Scenarios.deleteFileModeCases,
      Scenarios.morReadLayoutFormatCases).flatten

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

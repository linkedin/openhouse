package harness

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

final class CaseCatalogTest {
  @Test
  def catalogContainsEachCoreScenarioInStableOrder(): Unit = {
    assertEquals(
      List("dataTypeCases", "dmlCases", "dmlValidationCases"),
      Catalog.foundationContributions.map(_._1))
    assertEquals(
      List(
        "types.roundtrip @ types-unpartitioned/parquet",
        "types.nulls @ types-unpartitioned/parquet",
        "types.specialFloats @ types-unpartitioned/parquet",
        "types.boundaries @ types-unpartitioned/parquet",
        "types.unicodeAndEmpty @ types-unpartitioned/parquet",
        "types.roundtrip @ types-unpartitioned/orc",
        "types.nulls @ types-unpartitioned/orc",
        "types.specialFloats @ types-unpartitioned/orc",
        "types.boundaries @ types-unpartitioned/orc",
        "types.unicodeAndEmpty @ types-unpartitioned/orc",
        "read.projection @ parquet",
        "insert.into @ parquet",
        "insert.overwrite @ parquet",
        "delete.byPredicate @ parquet",
        "update.byPredicate @ parquet",
        "merge.upsert @ parquet",
        "read.projection @ orc",
        "insert.into @ orc",
        "insert.overwrite @ orc",
        "delete.byPredicate @ orc",
        "update.byPredicate @ orc",
        "merge.upsert @ orc",
        "dmlValidation.nonExistentColumn @ parquet",
        "dmlValidation.nonDeterministicDelete @ parquet",
        "dmlValidation.nonDeterministicUpdate @ parquet",
        "dmlValidation.insertArity @ parquet",
        "dmlValidation.mergeConflictingUpdates @ parquet",
        "dmlValidation.mergeCardinalityViolation @ parquet",
        "dmlValidation.nonExistentColumn @ orc",
        "dmlValidation.nonDeterministicDelete @ orc",
        "dmlValidation.nonDeterministicUpdate @ orc",
        "dmlValidation.insertArity @ orc",
        "dmlValidation.mergeConflictingUpdates @ orc",
        "dmlValidation.mergeCardinalityViolation @ orc"),
      Catalog.foundationContributions.flatMap { case (_, contribution) => contribution.map(_.id) })

    val duplicateCaseIds = Catalog.caseIds
      .groupBy(identity)
      .collect {
        case (caseId, occurrences) if occurrences.size > 1 => caseId
      }
      .toList
      .sorted

    assertTrue(
      duplicateCaseIds.isEmpty,
      s"case IDs must be unique; duplicates=${duplicateCaseIds.mkString(", ")}")
  }

  @Test
  def layoutsReadTheConfiguredDataSourceAtExecutionTime(): Unit = {
    val originalDataSource = Scenarios.dataSource
    try {
      val layouts = Scenarios.layouts
      val typesLayouts = Scenarios.typesLayouts
      Catalog.cases

      Scenarios.dataSource = "openhouse"
      assertTrue(layouts.forall(_.create("db.t").contains(" USING openhouse ")))
      assertTrue(typesLayouts.forall(_.create("db.t").contains(" USING openhouse ")))

      Scenarios.dataSource = "alternate"
      assertTrue(layouts.forall(_.create("db.t").contains(" USING alternate ")))
      assertTrue(typesLayouts.forall(_.create("db.t").contains(" USING alternate ")))
    } finally {
      Scenarios.dataSource = originalDataSource
    }
  }
}

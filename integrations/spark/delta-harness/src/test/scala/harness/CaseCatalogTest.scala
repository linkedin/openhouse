package harness

import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

final class CaseCatalogTest {
  @Test
  def everyCaseIdIsUnique(): Unit = {
    val duplicateCaseIds = ScenarioCatalog.caseIds
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
}

package harness

import java.nio.charset.StandardCharsets
import java.security.MessageDigest

import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.Test

final class CaseCatalogTest {
  private val expectedCaseCount = 2574
  private val expectedCatalogSha256 =
    "9e5ec513f2bbc775469154c8d1cf45e14654af2fca0e0f29b4bba6acae286a0a"

  @Test
  def orderedCaseCatalogMatchesBaseline(): Unit = {
    val caseIds = Plan.caseIds
    val actualCatalogSha256 = sha256(caseIds.mkString("\n"))
    val duplicateCaseIds = caseIds.groupBy(identity).collect {
      case (caseId, occurrences) if occurrences.size > 1 => caseId
    }.toList.sorted

    assertTrue(
      duplicateCaseIds.isEmpty,
      s"case IDs must be unique; duplicates=${duplicateCaseIds.mkString(", ")}")
    assertEquals(
      expectedCaseCount,
      caseIds.size,
      s"ordered case catalog changed; count=${caseIds.size}, sha256=$actualCatalogSha256")
    assertEquals(
      expectedCatalogSha256,
      actualCatalogSha256,
      s"ordered case catalog changed; count=${caseIds.size}, sha256=$actualCatalogSha256")
  }

  private def sha256(value: String): String =
    MessageDigest
      .getInstance("SHA-256")
      .digest(value.getBytes(StandardCharsets.UTF_8))
      .map(byte => f"$byte%02x")
      .mkString
}

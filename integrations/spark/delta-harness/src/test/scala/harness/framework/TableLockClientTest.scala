package harness

import com.linkedin.openhouse.relocated.org.springframework.http.{HttpHeaders, HttpStatus, ResponseEntity}
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException
import java.nio.charset.StandardCharsets.UTF_8
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

final class TableLockClientTest {
  @Test
  def successfulVoidResponseRetainsItsStatus(): Unit = {
    val response = new ResponseEntity[Void](HttpStatus.CREATED)

    assertEquals(
      TableLockResponse(HttpStatus.CREATED.value(), ""),
      GeneratedTableLockClient.translate(response))
  }

  @Test
  def failedResponseRetainsItsStatusAndDiagnosticText(): Unit = {
    val responseBody = """{"message":"table is locked"}"""
    val failure = WebClientResponseException.create(
      423,
      "Locked",
      HttpHeaders.EMPTY,
      responseBody.getBytes(UTF_8),
      UTF_8)

    assertEquals(
      TableLockResponse(423, responseBody),
      GeneratedTableLockClient.translate(throw failure))
  }
}

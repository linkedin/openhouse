package harness

import com.linkedin.openhouse.gen.client.ssl.TablesApiClientFactory
import com.linkedin.openhouse.gen.tables.client.api.TableApi
import com.linkedin.openhouse.gen.tables.client.model.CreateUpdateLockRequestBody
import com.linkedin.openhouse.relocated.org.springframework.http.ResponseEntity
import com.linkedin.openhouse.relocated.org.springframework.web.reactive.function.client.WebClientResponseException

/** The status and diagnostic response text returned by a table-lock operation. */
final case class TableLockResponse(statusCode: Int, diagnosticText: String)

/** The lock operations the behavioral scenarios require from the OpenHouse Tables service. */
trait TableLockClient {
  def createLock(databaseId: String, tableId: String): TableLockResponse
  def deleteLock(databaseId: String, tableId: String): TableLockResponse
}

object TableLockClient {
  /**
   * Creates the authenticated Tables client. HTTPS resolves the truststore through `TRUSTSTORE_LOCATION` when the
   * explicit value is empty; embedded HTTP bypasses truststore setup.
   */
  def create(tablesUri: String, authorizationToken: String): TableLockClient = {
    val apiClient =
      TablesApiClientFactory.getInstance().createApiClient(tablesUri, authorizationToken, "")
    new GeneratedTableLockClient(new TableApi(apiClient))
  }
}

private[harness] final class GeneratedTableLockClient(tableApi: TableApi) extends TableLockClient {
  override def createLock(databaseId: String, tableId: String): TableLockResponse =
    GeneratedTableLockClient.translate(
      tableApi
        .createLockV1WithHttpInfo(
          databaseId,
          tableId,
          new CreateUpdateLockRequestBody().locked(true))
        .block())

  override def deleteLock(databaseId: String, tableId: String): TableLockResponse =
    GeneratedTableLockClient.translate(
      tableApi.deleteLockV1WithHttpInfo(databaseId, tableId).block())
}

private[harness] object GeneratedTableLockClient {
  def translate(request: => ResponseEntity[Void]): TableLockResponse =
    try {
      TableLockResponse(request.getStatusCodeValue, "")
    } catch {
      case failure: WebClientResponseException =>
        TableLockResponse(failure.getRawStatusCode, failure.getResponseBodyAsString)
    }
}

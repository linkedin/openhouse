package com.linkedin.openhouse.spark.e2e.extensions;

import static com.linkedin.openhouse.spark.SparkTestBase.*;
import static org.junit.jupiter.api.Assertions.*;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.javaclient.exception.TableLockException;
import com.linkedin.openhouse.spark.SparkTestBase;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.RecordedRequest;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@ExtendWith(SparkTestBase.class)
public class LockUnlockStatementE2ETest {
  private static final String TABLE_PATH = "/v1/databases/db/tables/table";
  private static final String LOCK_PATH = TABLE_PATH + "/lock";

  @BeforeEach
  public void drainRecordedRequests() throws InterruptedException {
    Optional<RecordedRequest> recordedRequest =
        Optional.ofNullable(mockTableService.takeRequest(10, TimeUnit.MILLISECONDS));
    while (recordedRequest.isPresent()) {
      recordedRequest =
          Optional.ofNullable(mockTableService.takeRequest(10, TimeUnit.MILLISECONDS));
    }
  }

  @Test
  public void testLegacyLockRequest() throws Exception {
    mockTableService.enqueue(new MockResponse().setResponseCode(201));

    assertDoesNotThrow(() -> spark.sql("LOCK TABLE openhouse.db.table"));

    RecordedRequest request = takeRequest();
    assertEquals("POST", request.getMethod());
    assertEquals(LOCK_PATH, request.getPath());
    JsonObject requestBody = readRequestBody(request);
    assertTrue(requestBody.get("locked").getAsBoolean());
    assertEquals(0, requestBody.get("expirationInDays").getAsInt());
    assertTrue(requestBody.get("creationTime").getAsLong() > 0);
    assertEquals(Optional.empty(), optionalString(requestBody, "message"));
    assertEquals(Optional.of("LEGACY"), optionalString(requestBody, "reason"));
    assertEquals(Optional.empty(), optionalString(requestBody, "expectedTableUUID"));
  }

  @Test
  public void testLegacyUnlockRequest() throws Exception {
    mockTableService.enqueue(new MockResponse().setResponseCode(204));

    assertDoesNotThrow(() -> spark.sql("UNLOCK TABLE openhouse.db.table"));

    RecordedRequest request = takeRequest();
    assertEquals("DELETE", request.getMethod());
    assertEquals(LOCK_PATH, request.getPath());
    assertEquals(0, request.getBodySize());
  }

  @Test
  public void testReasonedLockRequestIncludesMessageAndGeneration() throws Exception {
    mockTableService.enqueue(jsonResponse(200, "{\"tableUUID\":\"generation\",\"policies\":null}"));
    mockTableService.enqueue(new MockResponse().setResponseCode(201));

    assertDoesNotThrow(
        () ->
            spark.sql(
                "LOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP "
                    + "MESSAGE 'This table is locked for cleanup'"));

    RecordedRequest getRequest = takeRequest();
    assertEquals("GET", getRequest.getMethod());
    assertEquals(TABLE_PATH, getRequest.getPath());

    RecordedRequest lockRequest = takeRequest();
    assertEquals("POST", lockRequest.getMethod());
    assertEquals(LOCK_PATH, lockRequest.getPath());
    JsonObject requestBody = readRequestBody(lockRequest);
    assertEquals(Optional.of("TIER3_AUTO_CLEANUP"), optionalString(requestBody, "reason"));
    assertEquals(Optional.of("generation"), optionalString(requestBody, "expectedTableUUID"));
    assertEquals(
        Optional.of("This table is locked for cleanup"), optionalString(requestBody, "message"));
  }

  @Test
  public void testReasonedUnlockRequest() throws Exception {
    mockTableService.enqueue(
        jsonResponse(
            200,
            "{\"tableUUID\":\"generation\",\"policies\":{\"lockState\":{"
                + "\"locked\":true,\"reason\":\"TIER3_AUTO_CLEANUP\","
                + "\"lockOwner\":\"lock-owner\",\"tableUUID\":\"generation\"}}}"));
    mockTableService.enqueue(new MockResponse().setResponseCode(204));

    assertDoesNotThrow(
        () -> spark.sql("UNLOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP"));

    RecordedRequest getRequest = takeRequest();
    assertEquals("GET", getRequest.getMethod());
    assertEquals(TABLE_PATH, getRequest.getPath());

    RecordedRequest unlockRequest = takeRequest();
    assertEquals("DELETE", unlockRequest.getMethod());
    assertEquals(LOCK_PATH + "/TIER3_AUTO_CLEANUP", unlockRequest.getRequestUrl().encodedPath());
    assertEquals("generation", unlockRequest.getRequestUrl().queryParameter("expectedTableUUID"));
    assertEquals("lock-owner", unlockRequest.getRequestUrl().queryParameter("lockOwner"));
    assertEquals(0, unlockRequest.getBodySize());
  }

  @Test
  public void testReasonedUnlockWithoutActiveLockIsIdempotent() throws Exception {
    mockTableService.enqueue(jsonResponse(200, "{\"tableUUID\":\"generation\",\"policies\":{}}"));

    assertDoesNotThrow(
        () -> spark.sql("UNLOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP"));

    assertEquals("GET", takeRequest().getMethod());
    assertNull(mockTableService.takeRequest(200, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testBlankMessageFailsBeforeMutation() throws Exception {
    TableLockException exception =
        assertThrows(
            TableLockException.class,
            () ->
                spark.sql(
                    "LOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP MESSAGE '   '"));

    assertTrue(exception.getMessage().contains("non-whitespace"));
    assertNull(mockTableService.takeRequest(200, TimeUnit.MILLISECONDS));
  }

  @ParameterizedTest
  @ValueSource(strings = {"UNKNOWN", "LOCK", "LEGACY", "legacy"})
  public void testUnsupportedLockReasonFailsBeforeRequest(String reason) throws Exception {
    TableLockException exception =
        assertThrows(
            TableLockException.class,
            () -> spark.sql("LOCK TABLE openhouse.db.table WITH REASON " + reason));

    assertTrue(exception.getMessage().contains("Unsupported table lock reason"));
    assertTrue(exception.getMessage().endsWith("Supported reasons: TIER3_AUTO_CLEANUP"));
    assertNull(mockTableService.takeRequest(200, TimeUnit.MILLISECONDS));
  }

  @ParameterizedTest
  @ValueSource(strings = {"UNKNOWN", "LEGACY"})
  public void testUnsupportedUnlockReasonFailsBeforeRequest(String reason) throws Exception {
    TableLockException exception =
        assertThrows(
            TableLockException.class,
            () -> spark.sql("UNLOCK TABLE openhouse.db.table WITH REASON " + reason));

    assertTrue(exception.getMessage().contains("Supported reasons: TIER3_AUTO_CLEANUP"));
    assertNull(mockTableService.takeRequest(200, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testLegacyLockReportedByServiceRejectsReasonedUnlock() throws Exception {
    mockTableService.enqueue(
        jsonResponse(
            200,
            "{\"tableUUID\":\"generation\",\"policies\":{\"lockState\":{"
                + "\"locked\":true,\"reason\":\"LEGACY\"}}}"));

    TableLockException exception =
        assertThrows(
            TableLockException.class,
            () -> spark.sql("UNLOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP"));

    assertTrue(exception.getMessage().contains("has a legacy lock"));
    assertEquals("GET", takeRequest().getMethod());
    assertNull(mockTableService.takeRequest(200, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testUnrecognizedLockReasonFailsClosed() throws Exception {
    mockTableService.enqueue(
        jsonResponse(
            200,
            "{\"tableUUID\":\"generation\",\"policies\":{\"lockState\":{"
                + "\"locked\":true,\"reason\":\"FUTURE_REASON\"}}}"));

    TableLockException exception =
        assertThrows(
            TableLockException.class,
            () -> spark.sql("UNLOCK TABLE openhouse.db.table WITH REASON TIER3_AUTO_CLEANUP"));

    assertTrue(exception.getMessage().contains("does not recognize"));
    assertEquals("GET", takeRequest().getMethod());
    assertNull(mockTableService.takeRequest(200, TimeUnit.MILLISECONDS));
  }

  @ParameterizedTest
  @ValueSource(ints = {400, 403, 409, 500})
  public void testMutationErrorsPreserveStatusAndResponseBody(int statusCode) throws Exception {
    mockTableService.enqueue(
        jsonResponse(
            statusCode,
            "{\"status\":\"ERROR\",\"message\":\"lock mutation failed with " + statusCode + "\"}"));

    TableLockException exception =
        assertThrows(TableLockException.class, () -> spark.sql("LOCK TABLE openhouse.db.table"));

    assertEquals(
        Integer.valueOf(statusCode), exception.getStatusCode().orElseThrow(AssertionError::new));
    assertTrue(exception.getResponseBody().orElse("").contains(Integer.toString(statusCode)));
    assertEquals(LOCK_PATH, takeRequest().getPath());
    assertNull(mockTableService.takeRequest(200, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testMultiLevelNamespaceFailsBeforeRequest() throws Exception {
    TableLockException exception =
        assertThrows(TableLockException.class, () -> spark.sql("LOCK TABLE openhouse.a.b.table"));

    assertTrue(exception.getMessage().contains("Input namespace has more than one levels"));
    assertNull(mockTableService.takeRequest(200, TimeUnit.MILLISECONDS));
  }

  private JsonObject readRequestBody(RecordedRequest request) {
    return new Gson().fromJson(request.getBody().readUtf8(), JsonObject.class);
  }

  private Optional<String> optionalString(JsonObject body, String field) {
    return Optional.ofNullable(body.get(field))
        .filter(value -> !value.isJsonNull())
        .map(JsonElement::getAsString);
  }

  private RecordedRequest takeRequest() throws InterruptedException {
    return Optional.ofNullable(mockTableService.takeRequest(2, TimeUnit.SECONDS))
        .orElseThrow(() -> new AssertionError("Expected a Tables service request"));
  }

  private MockResponse jsonResponse(int statusCode, String body) {
    return new MockResponse()
        .setResponseCode(statusCode)
        .setBody(body)
        .addHeader("Content-Type", "application/json");
  }
}

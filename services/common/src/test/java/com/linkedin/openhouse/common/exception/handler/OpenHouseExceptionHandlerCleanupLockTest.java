package com.linkedin.openhouse.common.exception.handler;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.CleanupLockAccessDeniedException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class OpenHouseExceptionHandlerCleanupLockTest {
  private static final String MESSAGE =
      "Table db.table is locked for TIER3_AUTO_CLEANUP. Promote to Tier 2 or use reason-targeted unlock.";
  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void cleanupDenialPreservesTheErrorBody(boolean withCause) {
    CleanupLockAccessDeniedException exception = new CleanupLockAccessDeniedException(MESSAGE);
    exception.setStackTrace(
        new StackTraceElement[] {
          new StackTraceElement(
              "LockPolicyValidator", "checkCleanupAccess", "LockPolicyValidator.java", 1)
        });
    if (withCause) {
      IllegalStateException cause = new IllegalStateException("original cause");
      cause.setStackTrace(new StackTraceElement[0]);
      exception.initCause(cause);
    }

    ResponseEntity<ErrorResponseBody> response =
        handler.handleUnsupportedClientOperationException(exception);
    assertEquals(HttpStatus.LOCKED, response.getStatusCode());
    ErrorResponseBody body = response.getBody();
    assertNotNull(body);
    assertEquals(HttpStatus.LOCKED, body.getStatus());
    assertEquals("Locked", body.getError());
    assertEquals(MESSAGE, body.getMessage());
    assertEquals(withCause ? "original cause" : "Not Available", body.getCause());
    assertEquals(ExceptionUtils.getStackTrace(exception), body.getStacktrace());
  }

  @ParameterizedTest
  @EnumSource(
      value = UnsupportedClientOperationException.Operation.class,
      names = {"LOCKED_TABLE_OPERATION", "PARTITION_EVOLUTION"})
  void legacyAndUnrelatedUnsupportedOperationsRemainBadRequests(
      UnsupportedClientOperationException.Operation operation) {
    ResponseEntity<ErrorResponseBody> response =
        handler.handleUnsupportedClientOperationException(
            new UnsupportedClientOperationException(operation, MESSAGE));
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertEquals(MESSAGE, response.getBody().getMessage());
    assertEquals("Bad Request", response.getBody().getError());
  }
}

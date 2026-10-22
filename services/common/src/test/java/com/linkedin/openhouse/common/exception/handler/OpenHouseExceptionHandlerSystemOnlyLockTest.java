package com.linkedin.openhouse.common.exception.handler;

import static org.junit.jupiter.api.Assertions.*;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.SystemOnlyLockAccessDeniedException;
import com.linkedin.openhouse.common.exception.UnsupportedClientOperationException;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

class OpenHouseExceptionHandlerSystemOnlyLockTest {
  private static final String MESSAGE =
      "Table db.table has a SYSTEM_ONLY lock. Use the reason-targeted OpenHouse unlock endpoint as an authorized lock administrator.";
  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  @Test
  void systemOnlyDenialPreservesTheErrorBody() {
    ResponseEntity<ErrorResponseBody> response =
        handler.handleUnsupportedClientOperationException(
            new SystemOnlyLockAccessDeniedException(MESSAGE));
    assertEquals(HttpStatus.LOCKED, response.getStatusCode());
    ErrorResponseBody body = response.getBody();
    assertNotNull(body);
    assertEquals(HttpStatus.LOCKED, body.getStatus());
    assertEquals("Locked", body.getError());
    assertEquals(MESSAGE, body.getMessage());
  }

  @Test
  void legacyLockedOperationsRemainBadRequests() {
    ResponseEntity<ErrorResponseBody> response =
        handler.handleUnsupportedClientOperationException(
            new UnsupportedClientOperationException(
                UnsupportedClientOperationException.Operation.LOCKED_TABLE_OPERATION, MESSAGE));
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertEquals(MESSAGE, response.getBody().getMessage());
    assertEquals("Bad Request", response.getBody().getError());
  }
}

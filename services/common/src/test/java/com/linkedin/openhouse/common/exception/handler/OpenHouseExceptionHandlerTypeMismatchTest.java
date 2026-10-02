package com.linkedin.openhouse.common.exception.handler;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

class OpenHouseExceptionHandlerTypeMismatchTest {
  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  @Test
  void nonEnumMismatchKeepsTheDefaultResponse() {
    ResponseEntity<Object> response =
        handler.handleTypeMismatch(
            new MethodArgumentTypeMismatchException("abc", Integer.class, "limit", null, null),
            new HttpHeaders(),
            HttpStatus.BAD_REQUEST,
            null);
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertNull(response.getBody());
  }
}

package com.linkedin.openhouse.common.exception.handler;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

class OpenHouseExceptionHandlerTypeMismatchTest {
  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  @ParameterizedTest
  @ValueSource(classes = {Integer.class, HttpStatus.class})
  void typeMismatchKeepsTheDefaultResponse(Class<?> requiredType) throws Exception {
    ResponseEntity<Object> response =
        handler.handleException(
            new MethodArgumentTypeMismatchException("invalid", requiredType, "value", null, null),
            null);
    assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
    assertNull(response.getBody());
  }
}

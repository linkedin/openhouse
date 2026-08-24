package com.linkedin.openhouse.common.exception.handler;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import javax.persistence.PersistenceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.jpa.JpaSystemException;

public class OpenHouseExceptionHandlerTest {

  private static final String CORRUPT_MSG =
      "Column user_table_row.entity_type holds unrecognized value [TÁBLE]; "
          + "only TABLE, VIEW (in any case) and NULL are valid";

  private static final String HIBERNATE_MSG = "Error attempting to apply AttributeConverter";

  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  @Test
  public void testCorruptEntityTypeIsServerErrorWithDiagnostic() {
    CorruptEntityTypeException corrupt =
        new CorruptEntityTypeException(CORRUPT_MSG, new IllegalArgumentException("TÁBLE"));

    ResponseEntity<ErrorResponseBody> response = handler.handleCorruptEntityTypeException(corrupt);

    assertServerErrorCarryingDiagnostic(response);
    Assertions.assertEquals("TÁBLE", response.getBody().getCause());
  }

  /** The shape Hibernate produces when the attribute converter fails mid-result-set. */
  @Test
  public void testJpaSystemExceptionUnwrapsToCorruptEntityTypeDiagnostic() {
    JpaSystemException wrapped =
        new JpaSystemException(
            new PersistenceException(
                HIBERNATE_MSG,
                new CorruptEntityTypeException(
                    CORRUPT_MSG, new IllegalArgumentException("TÁBLE"))));

    ResponseEntity<ErrorResponseBody> response =
        handler.handleWrappedCorruptEntityTypeException(wrapped);

    assertServerErrorCarryingDiagnostic(response);
    Assertions.assertFalse(response.getBody().getMessage().contains(HIBERNATE_MSG));
  }

  /** The other wrapper the JPA translator can pick, given the exception's IAE ancestry. */
  @Test
  public void testInvalidDataAccessApiUsageExceptionUnwrapsToCorruptEntityTypeDiagnostic() {
    InvalidDataAccessApiUsageException wrapped =
        new InvalidDataAccessApiUsageException(
            HIBERNATE_MSG,
            new CorruptEntityTypeException(CORRUPT_MSG, new IllegalArgumentException("TÁBLE")));

    ResponseEntity<ErrorResponseBody> response =
        handler.handleWrappedCorruptEntityTypeException(wrapped);

    assertServerErrorCarryingDiagnostic(response);
  }

  @Test
  public void testDeeplyNestedCorruptEntityTypeIsStillUnwrapped() {
    JpaSystemException wrapped =
        new JpaSystemException(
            new PersistenceException(
                HIBERNATE_MSG,
                new IllegalStateException(
                    "outer",
                    new RuntimeException(
                        "inner",
                        new CorruptEntityTypeException(
                            CORRUPT_MSG, new IllegalArgumentException("TÁBLE"))))));

    ResponseEntity<ErrorResponseBody> response =
        handler.handleWrappedCorruptEntityTypeException(wrapped);

    assertServerErrorCarryingDiagnostic(response);
  }

  @Test
  public void testUnrelatedDataAccessExceptionKeepsGenericBody() {
    JpaSystemException unrelated =
        new JpaSystemException(new PersistenceException("connection reset"));

    ResponseEntity<ErrorResponseBody> response =
        handler.handleWrappedCorruptEntityTypeException(unrelated);
    ResponseEntity<ErrorResponseBody> generic = handler.handleGenericException(unrelated);

    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    Assertions.assertEquals(generic.getBody().getMessage(), response.getBody().getMessage());
    Assertions.assertEquals(generic.getBody().getCause(), response.getBody().getCause());
    Assertions.assertFalse(response.getBody().getMessage().contains(CORRUPT_MSG));
  }

  @Test
  public void testCyclicCauseChainTerminates() {
    ResponseEntity<ErrorResponseBody> response =
        handler.handleWrappedCorruptEntityTypeException(
            new JpaSystemException(new SelfCausedException("cycle")));

    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    Assertions.assertFalse(response.getBody().getMessage().contains(CORRUPT_MSG));
  }

  private void assertServerErrorCarryingDiagnostic(ResponseEntity<ErrorResponseBody> response) {
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    ErrorResponseBody body = response.getBody();
    Assertions.assertNotNull(body);
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, body.getStatus());
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(), body.getError());
    Assertions.assertEquals(CORRUPT_MSG, body.getMessage());
    Assertions.assertNotNull(body.getStacktrace());
  }

  /**
   * {@link Throwable#initCause} forbids a self-referential cause, so the cycle is expressed by
   * overriding the accessor.
   */
  private static class SelfCausedException extends RuntimeException {
    SelfCausedException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }
}

package com.linkedin.openhouse.common.exception.handler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import java.lang.reflect.Field;
import java.util.Arrays;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

/**
 * The extraction is only safe if the shared advice actually routes through the factory; otherwise a
 * scoped advice composing it would stop producing indistinguishable bodies.
 */
public class OpenHouseExceptionHandlerDelegationTest {

  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  private final ErrorResponseBodyFactory factory = new ErrorResponseBodyFactory();

  /** Structural: the algorithm lives in the factory, and the advice holds one. */
  @Test
  public void testAdviceHoldsTheFactoryAndNoLongerOwnsTheFormattingConstants() {
    assertThat(
            Arrays.stream(OpenHouseExceptionHandler.class.getDeclaredFields()).map(Field::getType))
        .as("the advice must compose the extracted factory")
        .contains(ErrorResponseBodyFactory.class);

    assertThat(
            Arrays.stream(OpenHouseExceptionHandler.class.getDeclaredFields()).map(Field::getName))
        .as("the abbreviation cap and cause fallback are the factory's to own")
        .doesNotContain("STACKTRACE_MAX_WIDTH", "CAUSE_NOT_AVAILABLE");
  }

  /**
   * Behavioural: a private copy of the algorithm would drift from this the moment either changed.
   */
  @Test
  public void testGenericRenderingIsIdenticalToTheFactoryOutput() {
    RuntimeException exception =
        new RuntimeException("boom", new IllegalStateException("immediate cause"));

    ResponseEntity<ErrorResponseBody> response = handler.handleGenericException(exception);
    ErrorResponseBody expected = factory.genericServerError(exception);

    Assertions.assertEquals(expected.getStatus(), response.getBody().getStatus());
    Assertions.assertEquals(expected.getError(), response.getBody().getError());
    Assertions.assertEquals(expected.getMessage(), response.getBody().getMessage());
    Assertions.assertEquals(expected.getCause(), response.getBody().getCause());
    Assertions.assertEquals(expected.getStacktrace(), response.getBody().getStacktrace());
  }

  @Test
  public void testMappedStatusRenderingUsesTheSameStackTraceAndCause() {
    NoSuchUserTableException notFound = new NoSuchUserTableException("db1", "tb1");

    ResponseEntity<ErrorResponseBody> response = handler.handleEntityNotFound(notFound);

    Assertions.assertEquals(
        factory.getAbbreviatedStackTrace(notFound), response.getBody().getStacktrace());
    Assertions.assertEquals(factory.getExceptionCause(notFound), response.getBody().getCause());
    Assertions.assertEquals(
        ErrorResponseBodyFactory.CAUSE_NOT_AVAILABLE, response.getBody().getCause());
  }

  /** Regression: the refactor must not change any existing status or message. */
  @Test
  public void testValidationFailureStillRendersItsOwnMessageAtBadRequest() {
    RequestValidationFailureException validationFailure =
        new RequestValidationFailureException("tableId cannot be empty");

    ResponseEntity<ErrorResponseBody> response = handler.handleBadRequest(validationFailure);

    Assertions.assertEquals(400, response.getStatusCodeValue());
    Assertions.assertEquals("Bad Request", response.getBody().getError());
    assertThat(response.getBody().getMessage()).contains("tableId cannot be empty");
    Assertions.assertEquals(
        factory.getAbbreviatedStackTrace(validationFailure), response.getBody().getStacktrace());
  }
}

package com.linkedin.openhouse.common.exception.handler;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.CodedApiException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

/**
 * Direct coverage of the generic {@link CodedApiException} mapping added to {@link
 * OpenHouseExceptionHandler}.
 *
 * <p>Deliberately free of any downstream vocabulary: {@code services/common} has no dependency on
 * {@code services/tables}, so this test builds anonymous coded exceptions rather than referencing a
 * view error code. That the view taxonomy reduces to these statuses is a tables-side concern.
 *
 * <p>This test lives in the handler's own package so the {@code protected} handler method is
 * reachable without reflection.
 */
public class CodedApiExceptionHandlerTest {

  /**
   * Every status a downstream taxonomy currently reduces to, plus two it does not, proving the
   * handler forwards whatever the exception reports rather than mapping a known set.
   */
  private enum RepresentativeStatus {
    BAD_REQUEST(HttpStatus.BAD_REQUEST),
    NOT_FOUND(HttpStatus.NOT_FOUND),
    CONFLICT(HttpStatus.CONFLICT),
    UNPROCESSABLE_ENTITY(HttpStatus.UNPROCESSABLE_ENTITY),
    SERVICE_UNAVAILABLE(HttpStatus.SERVICE_UNAVAILABLE),
    FORBIDDEN(HttpStatus.FORBIDDEN),
    GATEWAY_TIMEOUT(HttpStatus.GATEWAY_TIMEOUT);

    private final HttpStatus httpStatus;

    RepresentativeStatus(HttpStatus httpStatus) {
      this.httpStatus = httpStatus;
    }
  }

  private static final String FIXED_MESSAGE = "a fixed redacted failure message";

  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  private static CodedApiException codedException(HttpStatus httpStatus, Throwable cause) {
    return new CodedApiException(FIXED_MESSAGE, cause) {
      @Override
      public HttpStatus getHttpStatus() {
        return httpStatus;
      }
    };
  }

  @ParameterizedTest
  @EnumSource(RepresentativeStatus.class)
  public void codedExceptionKeepsItsOwnStatusAndMessage(RepresentativeStatus representativeStatus) {
    HttpStatus expected = representativeStatus.httpStatus;

    ResponseEntity<ErrorResponseBody> response =
        handler.handleCodedApiException(codedException(expected, null));

    Assertions.assertEquals(
        expected,
        response.getStatusCode(),
        "The response status must come from the exception, not from a fixed mapping.");

    ErrorResponseBody body = response.getBody();
    Assertions.assertNotNull(body);
    Assertions.assertEquals(expected, body.getStatus());
    Assertions.assertEquals(expected.getReasonPhrase(), body.getError());
    Assertions.assertEquals(
        FIXED_MESSAGE,
        body.getMessage(),
        "The message is copied verbatim: the handler must not decorate it, because callers rely on"
            + " it staying redacted.");
    Assertions.assertNotNull(body.getStacktrace());
  }

  @Test
  public void codedExceptionCarriesItsCauseIntoTheBody() {
    ResponseEntity<ErrorResponseBody> withCause =
        handler.handleCodedApiException(
            codedException(
                HttpStatus.SERVICE_UNAVAILABLE, new IllegalStateException("downstream")));
    Assertions.assertNotNull(withCause.getBody());
    Assertions.assertTrue(
        withCause.getBody().getCause().contains("downstream"),
        "A wrapped root cause must survive into the error body.");

    ResponseEntity<ErrorResponseBody> withoutCause =
        handler.handleCodedApiException(codedException(HttpStatus.NOT_FOUND, null));
    Assertions.assertNotNull(withoutCause.getBody());
    Assertions.assertNotNull(
        withoutCause.getBody().getCause(),
        "A cause-less exception still populates the field rather than omitting it.");
  }

  /**
   * The generic handler exists precisely so that a downstream taxonomy does not have to widen the
   * shared error body. If a new field ever appears here, that decision has been reversed and needs
   * its own review.
   */
  @Test
  public void errorResponseBodyShapeIsUnchanged() {
    Set<String> expectedFields =
        new LinkedHashSet<>(Arrays.asList("status", "error", "message", "stacktrace", "cause"));

    Set<String> actualFields =
        Arrays.stream(ErrorResponseBody.class.getDeclaredFields())
            .filter(field -> !field.isSynthetic())
            .filter(field -> !Modifier.isStatic(field.getModifiers()))
            .map(Field::getName)
            .collect(Collectors.toCollection(LinkedHashSet::new));

    Assertions.assertEquals(
        expectedFields,
        actualFields,
        "No error code or other new field may be added to the shared error body.");
  }
}

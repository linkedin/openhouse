package com.linkedin.openhouse.tables.exception;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;

import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.ViewSourceDialectMismatchException;
import java.util.stream.Stream;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.BadRequestException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;

public class ViewFailureTranslatorTest {

  static Stream<Arguments> writeFailures() {
    return Stream.of(
        Arguments.of(
            new CommitStateUnknownException(new RuntimeException("x")),
            ViewErrorCode.COMMIT_STATE_UNKNOWN),
        Arguments.of(
            new ViewSourceDialectMismatchException("db", "v"),
            ViewErrorCode.INVALID_VIEW_DEFINITION),
        Arguments.of(new AlreadyExistsException("x"), ViewErrorCode.VIEW_ALREADY_EXISTS),
        Arguments.of(new CommitFailedException("x"), ViewErrorCode.CONCURRENT_VIEW_MODIFICATION),
        Arguments.of(
            new HouseTableRepositoryStateUnknownException("x", null),
            ViewErrorCode.VIEW_SERVICE_UNAVAILABLE),
        Arguments.of(new BadRequestException("x"), ViewErrorCode.INTERNAL_VIEW_ERROR),
        Arguments.of(new IllegalStateException("x"), ViewErrorCode.INTERNAL_VIEW_ERROR));
  }

  @ParameterizedTest
  @MethodSource("writeFailures")
  public void writeFailuresMapToTypedCodesAndKeepTheCause(
      RuntimeException failure, ViewErrorCode expected) {
    ViewApiException translated =
        assertInstanceOf(ViewApiException.class, ViewFailureTranslator.forWrite(failure));
    assertEquals(expected, translated.getErrorCode());
    assertSame(failure, translated.getCause());
  }

  @Test
  public void writePassesThroughTypedAndAuthorizationFailures() {
    RuntimeException typed = new ViewApiException(ViewErrorCode.NO_SUCH_VIEW, "x");
    RuntimeException denied = new AccessDeniedException("x");
    RuntimeException authUnavailable = new AuthorizationServiceException("x");
    assertSame(typed, ViewFailureTranslator.forWrite(typed));
    assertSame(denied, ViewFailureTranslator.forWrite(denied));
    assertSame(authUnavailable, ViewFailureTranslator.forWrite(authUnavailable));
  }

  static Stream<Arguments> readFailures() {
    return Stream.of(
        Arguments.of(new HouseTableNotFoundException("x", null), ViewErrorCode.NO_SUCH_VIEW),
        Arguments.of(
            new HouseTableRepositoryStateUnknownException("x", null),
            ViewErrorCode.VIEW_SERVICE_UNAVAILABLE),
        Arguments.of(new IllegalStateException("x"), ViewErrorCode.INTERNAL_VIEW_ERROR));
  }

  @ParameterizedTest
  @MethodSource("readFailures")
  public void readFailuresMapToTypedCodesAndKeepTheCause(
      RuntimeException failure, ViewErrorCode expected) {
    ViewApiException translated = ViewFailureTranslator.forRead(failure);
    assertEquals(expected, translated.getErrorCode());
    assertSame(failure, translated.getCause());
  }
}

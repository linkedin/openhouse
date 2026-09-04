package com.linkedin.openhouse.tables.exception;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

/**
 * Construction-time invariants of the views exception hierarchy.
 *
 * <p>Runs as a plain JUnit 5 test: these types hold no Spring wiring, and the point of the class is
 * that the failures below happen at the throw site rather than anywhere a context could be
 * involved.
 */
public class ViewApiExceptionTest {

  /**
   * {@link ViewApiException#getHttpStatus()} dereferences the code, and it is called by {@code
   * OpenHouseExceptionHandler} while it is already building the response. A null code accepted at
   * construction would therefore surface as a {@code NullPointerException} inside the handler and
   * be reported as a generic 500, hiding both the real fault and the status the throw site
   * intended.
   */
  @Test
  public void constructionRejectsANullErrorCodeRatherThanFailingInsideTheHandler() {
    NullPointerException fromMessageConstructor =
        Assertions.assertThrows(
            NullPointerException.class, () -> new ViewApiException(null, "any message"));
    Assertions.assertTrue(
        fromMessageConstructor.getMessage().contains("ViewErrorCode"),
        "The failure must name the missing argument, not read as an anonymous NPE.");

    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ViewApiException(null, "any message", new RuntimeException("cause")),
        "The cause-carrying constructor must apply the same rule.");

    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ViewRequestValidationFailureException(null, Collections.singletonList("reason")),
        "The validation subclass must reject a null code at construction too.");

    Assertions.assertThrows(
        NullPointerException.class,
        () -> new ViewRequestValidationFailureException(null, "reason"));
  }

  /** A well-formed exception reports the code it was given and the status that code maps to. */
  @Test
  public void aWellFormedExceptionReportsItsCodeAndStatus() {
    ViewApiException exception = new ViewApiException(ViewErrorCode.NO_SUCH_VIEW, "view not found");

    Assertions.assertEquals(ViewErrorCode.NO_SUCH_VIEW, exception.getErrorCode());
    Assertions.assertEquals(HttpStatus.NOT_FOUND, exception.getHttpStatus());
    Assertions.assertEquals("view not found", exception.getMessage());
  }

  /**
   * The reason {@link ViewValidationErrorCode} exists: a validation failure that is not a bad
   * request must not be expressible. This freezes the mapping so a later code added to {@link
   * ViewErrorCode} cannot drift into or out of the validation subset unnoticed.
   */
  @Test
  public void everyValidationCodeMapsToABadRequestViewErrorCode() {
    Set<String> expected =
        setOf("INVALID_VIEW_DEFINITION", "UNSUPPORTED_VIEW_DIALECT", "UNSUPPORTED_VIEW_SCHEMA");

    Assertions.assertEquals(
        expected,
        Arrays.stream(ViewValidationErrorCode.values())
            .map(Enum::name)
            .collect(Collectors.toCollection(LinkedHashSet::new)),
        "ViewValidationErrorCode names exactly the 400-mapped codes.");

    for (ViewValidationErrorCode code : ViewValidationErrorCode.values()) {
      Assertions.assertEquals(
          code.name(),
          code.getViewErrorCode().name(),
          "Each validation code must name the identically named ViewErrorCode.");
      Assertions.assertEquals(
          HttpStatus.BAD_REQUEST,
          code.getViewErrorCode().getHttpStatus(),
          "ViewValidationErrorCode." + code.name() + " must map to a BAD_REQUEST code.");
    }

    Assertions.assertEquals(
        expected,
        Arrays.stream(ViewErrorCode.values())
            .filter(code -> code.getHttpStatus() == HttpStatus.BAD_REQUEST)
            .map(Enum::name)
            .collect(Collectors.toCollection(LinkedHashSet::new)),
        "The validation subset must stay exhaustive: every BAD_REQUEST ViewErrorCode is"
            + " expressible as a validation failure.");
  }

  /** Reasons are joined the way the tables API joins them, so both APIs read identically. */
  @Test
  public void accumulatedReasonsAreJoinedWithASemicolon() {
    ViewRequestValidationFailureException exception =
        new ViewRequestValidationFailureException(
            ViewValidationErrorCode.UNSUPPORTED_VIEW_DIALECT,
            Arrays.asList("first reason", "second reason"));

    Assertions.assertEquals("first reason; second reason", exception.getMessage());
    Assertions.assertEquals(ViewErrorCode.UNSUPPORTED_VIEW_DIALECT, exception.getErrorCode());
    Assertions.assertEquals(HttpStatus.BAD_REQUEST, exception.getHttpStatus());
  }

  private static Set<String> setOf(String... values) {
    return new LinkedHashSet<>(Arrays.asList(values));
  }
}

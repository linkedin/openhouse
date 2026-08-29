package com.linkedin.openhouse.common.exception.handler;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

/**
 * The extraction of the shared advice's formatting mechanics must be behavior-neutral, so this pins
 * the three properties any composing advice depends on: the body shape, the
 * immediate-cause-or-{@code "Not Available"} rule, and the abbreviation cap.
 */
public class ErrorResponseBodyFactoryTest {

  private final ErrorResponseBodyFactory factory = new ErrorResponseBodyFactory();

  @Test
  public void testBuildCarriesStatusReasonPhraseMessageAndCause() {
    ErrorResponseBody body =
        factory.build(
            HttpStatus.INTERNAL_SERVER_ERROR,
            "diagnostic message",
            new IllegalStateException("outer", new IllegalArgumentException("immediate cause")));

    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, body.getStatus());
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(), body.getError());
    Assertions.assertEquals("diagnostic message", body.getMessage());
    Assertions.assertEquals("immediate cause", body.getCause());
    Assertions.assertNotNull(body.getStacktrace());
    Assertions.assertTrue(body.getStacktrace().contains("IllegalStateException"));
  }

  /** The supplied message wins over the exception's own, which is what lets an advice re-render. */
  @Test
  public void testBuildPrefersTheSuppliedMessageOverTheExceptionMessage() {
    ErrorResponseBody body =
        factory.build(HttpStatus.NOT_FOUND, "supplied", new RuntimeException("exception message"));

    Assertions.assertEquals("supplied", body.getMessage());
  }

  /** Only the immediate cause is reported; a missing one is the literal fallback, never null. */
  @Test
  public void testCauselessExceptionReportsNotAvailable() {
    Assertions.assertEquals(
        ErrorResponseBodyFactory.CAUSE_NOT_AVAILABLE,
        factory.getExceptionCause(new RuntimeException("no cause")));
    Assertions.assertEquals(
        ErrorResponseBodyFactory.CAUSE_NOT_AVAILABLE,
        factory.build(HttpStatus.BAD_REQUEST, "m", new RuntimeException("no cause")).getCause());
  }

  /** The generic 500 message is the exception's own {@code toString()}, not its message. */
  @Test
  public void testGenericServerErrorRendersExceptionToString() {
    RuntimeException exception = new RuntimeException("boom");

    ErrorResponseBody body = factory.genericServerError(exception);

    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, body.getStatus());
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(), body.getError());
    Assertions.assertEquals(exception.toString(), body.getMessage());
    Assertions.assertNotNull(body.getStacktrace());
  }

  /**
   * A trace under the cap is returned byte-for-byte. The frames are synthetic so the fixture does
   * not depend on how deep the test runner's own stack happens to be.
   */
  @Test
  public void testShortStackTraceIsReturnedExactlyAsProduced() {
    RuntimeException exception = syntheticTrace(5);
    String full = ExceptionUtils.getStackTrace(exception);
    Assertions.assertTrue(
        full.length() <= ErrorResponseBodyFactory.STACKTRACE_MAX_WIDTH,
        "fixture must be under the cap: " + full.length());

    Assertions.assertEquals(full, factory.getAbbreviatedStackTrace(exception));
  }

  /**
   * Characterization of the algorithm the shared advice has always applied, asserted exactly
   * because this is an extract-and-delegate refactor. It keeps the leading 1500 characters, then
   * takes 600-character windows every 2000 starting at offset 6000, and finally takes the remaining
   * tail once fewer than 2600 characters are left. The ellipsis markers come from {@code
   * StringUtils.abbreviate}, so the windows are expressed through it: what is pinned here is the
   * offsets and widths the algorithm chooses, which is the part that would silently change.
   */
  @Test
  public void testLongStackTraceKeepsTheDocumentedRetainedAndSkippedRegions() {
    RuntimeException exception = syntheticTrace(190);
    String full = ExceptionUtils.getStackTrace(exception);
    Assertions.assertTrue(
        full.length() >= 10600 && full.length() < 12600,
        "fixture must land in the band where the tail is taken at offset 10600: " + full.length());
    // Tail width: whatever is left after the offset-10000 window, capped by the remaining budget
    // (6000 minus the 1500 and the three 600-character windows already taken).
    int tailWidth = Math.min(full.length() - 10600, 2700);

    String abbreviated = factory.getAbbreviatedStackTrace(exception);

    Assertions.assertEquals(
        StringUtils.abbreviate(full, 0, 1500)
            + StringUtils.abbreviate(full, 6000, 600)
            + StringUtils.abbreviate(full, 8000, 600)
            + StringUtils.abbreviate(full, 10000, 600)
            + StringUtils.abbreviate(full, 10600, tailWidth),
        abbreviated);
    // The gaps between the sampled windows really are dropped, not merely truncated away.
    Assertions.assertFalse(abbreviated.contains(full.substring(3000, 3200)));
    Assertions.assertFalse(abbreviated.contains(full.substring(7000, 7100)));
  }

  /**
   * Pre-existing quirk, pinned so the refactor cannot silently "fix" it and change every 500 body
   * on the wire: the running total advances in 600-character steps from 1500, so it never lands on
   * {@link ErrorResponseBodyFactory#STACKTRACE_MAX_WIDTH} exactly. For a very long trace the width
   * calculation goes negative, {@code StringUtils.abbreviate} rejects it, and the partial result
   * collected so far is returned — which can exceed the nominal cap.
   *
   * <p>Changing this is a coordinated common-contract decision, not part of the extraction.
   */
  @Test
  public void testVeryLongStackTraceReturnsThePartialResultAndExceedsTheNominalCap() {
    String abbreviated = factory.getAbbreviatedStackTrace(syntheticTrace(400));

    Assertions.assertTrue(
        abbreviated.length() > ErrorResponseBodyFactory.STACKTRACE_MAX_WIDTH,
        "the current algorithm overshoots the nominal cap here: " + abbreviated.length());
  }

  /** Deterministic stack trace of a controlled depth, independent of the caller's own stack. */
  private static RuntimeException syntheticTrace(int frames) {
    RuntimeException exception = new RuntimeException("synthetic");
    StackTraceElement[] elements = new StackTraceElement[frames];
    for (int frame = 0; frame < frames; frame++) {
      elements[frame] =
          new StackTraceElement(
              "com.linkedin.openhouse.Padded" + String.format("%04d", frame),
              "invoke",
              "Padded.java",
              1000 + frame);
    }
    exception.setStackTrace(elements);
    return exception;
  }
}

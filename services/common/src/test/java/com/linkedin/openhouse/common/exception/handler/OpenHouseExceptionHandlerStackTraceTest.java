package com.linkedin.openhouse.common.exception.handler;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.CorruptEntityTypeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.junit.jupiter.api.Test;
import org.springframework.http.ResponseEntity;

/**
 * Characterization of the shared advice's stack-trace abbreviation. The algorithm is deliberately
 * untouched by this change, so these pin what it already does rather than what it should do.
 */
public class OpenHouseExceptionHandlerStackTraceTest {

  private static final int STACKTRACE_MAX_WIDTH = 6000;
  private static final int TOP_LEVEL_WIDTH = 1500;
  private static final int MIDDLE_WIDTH = 600;
  private static final int FIRST_MIDDLE_OFFSET = 6000;
  private static final int MIDDLE_SKIP = 2000;

  private final OpenHouseExceptionHandler handler = new OpenHouseExceptionHandler();

  private String renderedStackTrace(CorruptEntityTypeException failure) {
    ResponseEntity<ErrorResponseBody> response = handler.handleCorruptEntityTypeException(failure);
    return response.getBody().getStacktrace();
  }

  /** Anything at or under the width is returned whole, character for character. */
  @Test
  public void testShortStackTraceIsReturnedExactly() {
    CorruptEntityTypeException failure =
        withSyntheticFrames(new CorruptEntityTypeException("short and sweet"), 3);
    String full = ExceptionUtils.getStackTrace(failure);
    assertThat(full.length()).isLessThanOrEqualTo(STACKTRACE_MAX_WIDTH);

    assertThat(renderedStackTrace(failure)).isEqualTo(full);
  }

  /**
   * A long trace is sampled: the top level in full-ish, then fixed windows every {@value
   * #MIDDLE_SKIP} characters from offset {@value #FIRST_MIDDLE_OFFSET}. The gaps between those
   * windows are dropped.
   */
  @Test
  public void testLongStackTraceRetainsTheSampledWindowsAndDropsTheGaps() {
    CorruptEntityTypeException failure =
        withSyntheticFrames(new CorruptEntityTypeException("deep"), 190);
    String full = ExceptionUtils.getStackTrace(failure);
    assertThat(full.length()).isGreaterThan(STACKTRACE_MAX_WIDTH);

    String rendered = renderedStackTrace(failure);

    assertThat(rendered).startsWith(StringUtils.abbreviate(full, 0, TOP_LEVEL_WIDTH));
    for (int offset = FIRST_MIDDLE_OFFSET;
        offset + MIDDLE_WIDTH <= full.length() - MIDDLE_SKIP;
        offset += MIDDLE_SKIP) {
      assertThat(rendered).contains(StringUtils.abbreviate(full, offset, MIDDLE_WIDTH));
    }
    // A region between the top level and the first middle window is skipped entirely.
    assertThat(rendered).doesNotContain(full.substring(3000, 3200));
  }

  /**
   * The width is a sampling parameter, not a cap: the running total steps past it rather than
   * landing on it, so the result can be longer. Pinned because it looks like a bug and is not.
   */
  @Test
  public void testRenderedLengthMayExceedTheNominalMaxWidth() {
    CorruptEntityTypeException failure =
        withSyntheticFrames(new CorruptEntityTypeException("deeper"), 400);
    assertThat(ExceptionUtils.getStackTrace(failure).length())
        .isGreaterThan(4 * STACKTRACE_MAX_WIDTH);

    assertThat(renderedStackTrace(failure).length()).isGreaterThan(STACKTRACE_MAX_WIDTH);
  }

  private static CorruptEntityTypeException withSyntheticFrames(
      CorruptEntityTypeException failure, int frameCount) {
    StackTraceElement[] frames = new StackTraceElement[frameCount];
    for (int i = 0; i < frameCount; i++) {
      frames[i] =
          new StackTraceElement(
              "com.linkedin.openhouse.synthetic.Frame" + i, "call", "Frame" + i + ".java", i + 1);
    }
    failure.setStackTrace(frames);
    return failure;
  }
}

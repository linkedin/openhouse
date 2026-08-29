package com.linkedin.openhouse.common.exception.handler;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.springframework.http.HttpStatus;

/**
 * The body/stack/cause formatting {@link OpenHouseExceptionHandler} applies, extracted so a
 * service-scoped advice can produce a byte-compatible {@link ErrorResponseBody} by composition
 * rather than by inheriting the shared advice and, with it, every one of its exception mappings.
 *
 * <p>Stateless and behavior-neutral: it renders exactly what the shared advice rendered before the
 * extraction, including the sampling algorithm below and the immediate-cause-or-{@value
 * #CAUSE_NOT_AVAILABLE} selection.
 */
public class ErrorResponseBodyFactory {

  public static final String CAUSE_NOT_AVAILABLE = "Not Available";

  /**
   * The abbreviation budget the sampling loop aims at. It is a target rather than a hard limit: the
   * running total advances from 1500 in 600-character steps and so never lands on this value
   * exactly, and for a sufficiently long trace the loop exits through its partial-return branch
   * with more than this many characters. Preserved deliberately — changing it would alter every 500
   * body on the wire and is a coordinated common-contract decision, not part of the extraction.
   */
  public static final int STACKTRACE_MAX_WIDTH = 6000;

  /** Builds a body at an arbitrary status, from a supplied diagnostic message. */
  public ErrorResponseBody build(HttpStatus status, String message, Throwable exception) {
    return ErrorResponseBody.builder()
        .status(status)
        .error(status.getReasonPhrase())
        .message(message)
        .stacktrace(getAbbreviatedStackTrace(exception))
        .cause(getExceptionCause(exception))
        .build();
  }

  /**
   * The generic 500 rendering: the message is the exception's own {@code toString()}, exactly as
   * {@code OpenHouseExceptionHandler.handleGenericException} produces it.
   */
  public ErrorResponseBody genericServerError(Throwable exception) {
    return build(HttpStatus.INTERNAL_SERVER_ERROR, exception.toString(), exception);
  }

  /**
   * Gets reduced size stacktrace. Keeps the leading 1500 characters, then samples 600-character
   * windows every 2000 from offset 6000 onwards, and finally takes the remaining tail. Returns
   * empty or partial stacktrace if there is an exception.
   *
   * <p>The result approaches but does not strictly respect {@link #STACKTRACE_MAX_WIDTH}; see that
   * field for why the overshoot is preserved rather than corrected.
   *
   * @param exception
   * @return String
   */
  public String getAbbreviatedStackTrace(Throwable exception) {
    String stackTrace = ExceptionUtils.getStackTrace(exception);
    if (StringUtils.isEmpty(stackTrace)) {
      return null;
    }
    // Return the complete stacktrace if it is already within the budget
    if (stackTrace.length() <= STACKTRACE_MAX_WIDTH) {
      return stackTrace;
    }
    StringBuilder builder = new StringBuilder();
    // Extract the first level stacktrace with max width of 1500 so that we get better view of top
    // level stacktrace
    builder.append(StringUtils.abbreviate(stackTrace, 0, 1500));
    // Extract minimal stacktrace from the middle levels
    int width = 600;
    // skip every 2000 characters
    int skipLength = 2000;
    // Start the next scan from 6000
    int startOffset = 6000;
    // So far 1500 is extracted from top level
    int abbreviatedLength = 1500;
    // Flag to track the deepest level
    boolean isDeepestLevel = false;
    while (startOffset + width <= stackTrace.length()) {
      try {
        builder.append(StringUtils.abbreviate(stackTrace, startOffset, width));
        abbreviatedLength += width;
        if (isDeepestLevel || abbreviatedLength == STACKTRACE_MAX_WIDTH) {
          break;
        }
        // Extract the deepest level of stacktrace from the remaining stacktrace
        if (startOffset + width + skipLength > stackTrace.length()) {
          // reset the skip length to width which is already extracted in this scan so that next
          // offset can be started
          // without skipping characters
          skipLength = width;
          // Determine the min characters that can be extracted
          width =
              Math.min(
                  stackTrace.length() - (startOffset + width),
                  STACKTRACE_MAX_WIDTH - abbreviatedLength);
          isDeepestLevel = true;
        }
        // Start the next start offset
        startOffset += skipLength;
      } catch (IllegalArgumentException ex) {
        return builder.toString();
      }
    }
    return builder.toString();
  }

  /** The immediate cause's message, or {@value #CAUSE_NOT_AVAILABLE} when there is no cause. */
  public String getExceptionCause(Throwable exception) {
    return exception.getCause() != null ? exception.getCause().getMessage() : CAUSE_NOT_AVAILABLE;
  }
}

package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.tables.audit.ViewRequestUriRedactor;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Servlet binding percent-decodes query parameter names, so every spelling that binds to {@code
 * pageToken} must be redacted from the audited URI, not only the literal one.
 */
public class ViewRequestUriRedactorTest {

  private static final String PATH = "/v1/databases/db/views";
  private static final String SECRET_A = "TOKEN_SECRET_A";
  private static final String SECRET_B = "TOKEN_SECRET_B";

  private final ViewRequestUriRedactor redactor = new ViewRequestUriRedactor();

  @Test
  public void literalPageTokenIsRedactedAndSafeFieldsAreKept() {
    String redacted = redactor.redact(PATH + "?pageToken=" + SECRET_A + "&sortBy=viewId&size=1");

    assertFalse(redacted.contains(SECRET_A), redacted);
    assertTrue(redacted.startsWith(PATH + "?"), redacted);
    assertTrue(redacted.contains("sortBy=viewId"), redacted);
    assertTrue(redacted.contains("size=1"), redacted);
  }

  /** Each name below decodes to {@code pageToken}. */
  @ParameterizedTest
  @ValueSource(
      strings = {"%70ageToken", "page%54oken", "%70%61%67%65%54%6f%6b%65%6e", "%70ageT%6Fken"})
  public void percentEncodedPageTokenNamesAreRedacted(String encodedName) {
    String redacted =
        redactor.redact(PATH + "?" + encodedName + "=" + SECRET_A + "&sortBy=viewId&size=1");

    assertFalse(redacted.contains(SECRET_A), redacted);
    assertTrue(redacted.contains("sortBy=viewId"), redacted);
    assertTrue(redacted.contains("size=1"), redacted);
  }

  @Test
  public void mixedLiteralAndEncodedDuplicatesAreAllRedacted() {
    String redacted =
        redactor.redact(
            PATH + "?pageToken=" + SECRET_A + "&size=2&%70ageToken=" + SECRET_B + "&sortBy=viewId");

    assertFalse(redacted.contains(SECRET_A), redacted);
    assertFalse(redacted.contains(SECRET_B), redacted);
    assertTrue(redacted.contains("size=2"), redacted);
    assertTrue(redacted.contains("sortBy=viewId"), redacted);
  }

  @Test
  public void percentEncodedTokenValueIsRedacted() {
    String redacted = redactor.redact(PATH + "?%70ageToken=" + "TOKEN%5FSECRET%5FA" + "&size=1");

    assertFalse(redacted.contains("TOKEN%5FSECRET%5FA"), redacted);
    assertFalse(redacted.contains(SECRET_A), redacted);
  }

  /**
   * A query that cannot be fully decoded must not let a token through, including a parameter whose
   * own name cannot be decoded.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "?%70ageToken=" + SECRET_A + "&size=%ZZ",
        "?size=1&%70ageToken=" + SECRET_A + "&sortBy=%E0%A4%A",
        "?%70ageToken=" + SECRET_A + "&%",
        "?pageToken%=" + SECRET_A + "&size=1"
      })
  public void malformedQueryFailsClosedForPageToken(String query) {
    String redacted = redactor.redact(PATH + query);

    assertFalse(redacted.contains(SECRET_A), redacted);
  }
}

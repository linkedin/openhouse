package com.linkedin.openhouse.tables.mock.audit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.linkedin.openhouse.common.audit.ServiceAuditUriRedactor;
import com.linkedin.openhouse.tables.audit.ViewRequestUriRedactor;
import java.util.Set;
import javax.servlet.http.HttpServletRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.web.util.ServletRequestPathUtils;

/**
 * Servlet binding percent-decodes query parameter names, so every spelling that binds to {@code
 * pageToken} must be redacted from the audited URI, not only the literal one.
 */
public class ViewRequestUriRedactorTest {

  private static final String PATH = "/v1/databases/db/views";
  private static final String SECRET_A = "TOKEN_SECRET_A";
  private static final String SECRET_B = "TOKEN_SECRET_B";

  private final ViewRequestUriRedactor redactor = new ViewRequestUriRedactor();

  /**
   * The URI redactor claims exactly the request paths MVC routes to the view list, whether MVC has
   * already parsed the request path or not. Classification leaves the request untouched: the raw
   * URI and attributes are unchanged, and a standalone request gains no parsed path.
   */
  @ParameterizedTest
  @MethodSource("com.linkedin.openhouse.tables.mock.audit.ViewAuditRouteCases#routes")
  public void claimsExactlyTheMvcViewListRoutes(
      String contextPath, String rawRequestUri, boolean collection, boolean viewRoute) {
    for (boolean cached : new boolean[] {false, true}) {
      MockHttpServletRequest request =
          ViewAuditRouteCases.request(contextPath, rawRequestUri, cached);
      Set<String> attributesBefore = ViewAuditRouteCases.attributeNames(request);

      assertEquals(collection, redactor.appliesTo(request), rawRequestUri + " cached=" + cached);

      assertEquals(rawRequestUri, request.getRequestURI());
      assertEquals(attributesBefore, ViewAuditRouteCases.attributeNames(request));
      assertEquals(cached, ServletRequestPathUtils.hasParsedRequestPath(request));
    }
  }

  /** A view list reached through MVC's parsed servlet mapping is classified by MVC's path. */
  @Test
  public void claimsTheViewListWithinMvcsParsedServletMapping() {
    HttpServletRequest request = ViewAuditRouteCases.servletPrefixedCollectionRequest();

    assertTrue(redactor.appliesTo(request));
  }

  /**
   * Classification never rewrites the audited path: an accepted alias keeps its raw spelling and
   * every safe field, and only the bound token value is replaced.
   */
  @ParameterizedTest
  @ValueSource(
      strings = {
        "/v1/databases/db/views/",
        "/v1/databases/db/%76iews",
        "/v1/databases/db/%76iews/"
      })
  public void claimedAliasKeepsItsRawSpellingWhileTheTokenIsReplaced(String rawRequestUri) {
    assertTrue(redactor.appliesTo(ViewAuditRouteCases.request("", rawRequestUri, false)));

    assertEquals(
        rawRequestUri
            + "?pageToken="
            + ServiceAuditUriRedactor.REDACTED_VALUE
            + "&sortBy=viewId&%70ageToken="
            + ServiceAuditUriRedactor.REDACTED_VALUE
            + "&size=1",
        redactor.redact(
            rawRequestUri
                + "?pageToken="
                + SECRET_A
                + "&sortBy=viewId&%70ageToken="
                + SECRET_B
                + "&size=1"));
  }

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

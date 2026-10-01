package com.linkedin.openhouse.tables.audit;

import com.linkedin.openhouse.common.audit.ServiceAuditUriRedactor;
import java.nio.charset.StandardCharsets;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;
import org.springframework.web.util.UriUtils;

/**
 * Keeps the view listing {@code pageToken} out of service audit events.
 *
 * <p>{@link com.linkedin.openhouse.common.audit.ServiceAuditAspect} audits the raw request URI
 * (including the query string) of every controller call, which for the view list route would retain
 * the caller's opaque {@code pageToken} verbatim &mdash; the same value {@link
 * com.linkedin.openhouse.tables.services.ViewPageTokenCodec} treats as an internal cursor, not a
 * value safe to retain in an audit trail. This replaces only the {@code pageToken} query value with
 * {@link #REDACTED_VALUE}; every other query field (notably {@code sortBy} and {@code size}) is
 * left intact, so an audit event still shows how a list request was shaped.
 *
 * <p>Applied to the raw query string rather than a parsed request, so a syntactically malformed or
 * percent-encoded token is redacted the same way a well-formed one is: the match is on the literal
 * {@code pageToken=} key, not on a successfully decoded value.
 */
@Component
public class ViewRequestUriRedactor implements ServiceAuditUriRedactor {

  private static final String VIEW_COLLECTION_PATTERN = "/v1/databases/*/views";

  private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

  /**
   * Matches a {@code pageToken} query parameter and captures its value, up to the next {@code &} or
   * the end of the string. Query parameters are {@code &}-delimited, so a token value can never
   * itself legitimately contain an unencoded {@code &}.
   */
  private static final Pattern PAGE_TOKEN_PARAM = Pattern.compile("([?&])pageToken=([^&]*)");

  @Override
  public boolean appliesTo(HttpServletRequest request) {
    String uri = request.getRequestURI();
    return uri != null && PATH_MATCHER.match(VIEW_COLLECTION_PATTERN, uri);
  }

  @Override
  public String redact(String uriAndQueryString) {
    if (uriAndQueryString == null || !uriAndQueryString.contains("pageToken=")) {
      return uriAndQueryString;
    }
    Matcher matcher = PAGE_TOKEN_PARAM.matcher(uriAndQueryString);
    StringBuilder redacted = new StringBuilder();
    while (matcher.find()) {
      String replacement =
          matcher.group(1)
              + "pageToken="
              + UriUtils.encodeQueryParam(REDACTED_VALUE, StandardCharsets.UTF_8);
      matcher.appendReplacement(redacted, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(redacted);
    return redacted.toString();
  }
}

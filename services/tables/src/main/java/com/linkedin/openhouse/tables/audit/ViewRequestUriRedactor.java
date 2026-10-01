package com.linkedin.openhouse.tables.audit;

import com.linkedin.openhouse.common.audit.ServiceAuditUriRedactor;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;
import org.springframework.util.AntPathMatcher;

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
 * <p>Servlet parameter binding percent-decodes query parameter <em>names</em> before matching them
 * against a controller's {@code @RequestParam}, so {@code %70ageToken} binds to the same {@code
 * pageToken} parameter as the literal spelling. Matching only the literal {@code pageToken=} key
 * would therefore miss an encoded (or duplicated, mixed literal/encoded) alias of the exact same
 * bound parameter &mdash; not a different parameter, just an alternate wire spelling of it. Each
 * {@code &}-delimited parameter is inspected independently: its name is decoded and compared, and
 * only a parameter whose name is undecodable or whose decoded name exactly matches {@code
 * pageToken} has its value replaced; every other parameter (and the rest of a mixed query) is left
 * exactly as sent.
 */
@Component
public class ViewRequestUriRedactor implements ServiceAuditUriRedactor {

  private static final String VIEW_COLLECTION_PATTERN = "/v1/databases/*/views";

  private static final AntPathMatcher PATH_MATCHER = new AntPathMatcher();

  private static final String PAGE_TOKEN_PARAM_NAME = "pageToken";

  @Override
  public boolean appliesTo(HttpServletRequest request) {
    String uri = request.getRequestURI();
    return uri != null && PATH_MATCHER.match(VIEW_COLLECTION_PATTERN, uri);
  }

  @Override
  public String redact(String uriAndQueryString) {
    if (uriAndQueryString == null) {
      return null;
    }
    int queryStart = uriAndQueryString.indexOf('?');
    if (queryStart < 0) {
      return uriAndQueryString;
    }
    String path = uriAndQueryString.substring(0, queryStart);
    String query = uriAndQueryString.substring(queryStart + 1);
    if (query.isEmpty()) {
      return uriAndQueryString;
    }

    String[] rawParams = query.split("&", -1);
    List<String> redactedParams = new ArrayList<>(rawParams.length);
    for (String rawParam : rawParams) {
      redactedParams.add(redactParam(rawParam));
    }
    return path + "?" + String.join("&", redactedParams);
  }

  private static String redactParam(String rawParam) {
    int eq = rawParam.indexOf('=');
    String rawName = eq >= 0 ? rawParam.substring(0, eq) : rawParam;
    String rawValue = eq >= 0 ? rawParam.substring(eq + 1) : null;

    if (rawValue != null && boundToPageToken(rawName)) {
      return rawName + "=" + REDACTED_VALUE;
    }
    return rawParam;
  }

  /**
   * Whether {@code rawName} is a spelling that servlet parameter binding would decode to {@code
   * pageToken}. An undecodable name is treated as a possible alias rather than ruled out: a caller
   * cannot smuggle a token past this redactor merely by corrupting its own percent-encoding.
   */
  private static boolean boundToPageToken(String rawName) {
    try {
      return PAGE_TOKEN_PARAM_NAME.equals(
          URLDecoder.decode(rawName, StandardCharsets.UTF_8.name()));
    } catch (UnsupportedEncodingException | IllegalArgumentException e) {
      return true;
    }
  }
}

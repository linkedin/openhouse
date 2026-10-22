package com.linkedin.openhouse.tables.config;

import java.util.Arrays;
import java.util.Collections;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import org.springframework.http.HttpHeaders;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;

/**
 * Reads {@link CatalogRequestContext} from the current request's headers. Outside a request every
 * option is off and the client is unknown.
 */
@Component
public class HttpCatalogRequestContext implements CatalogRequestContext {

  /** Spring's proxy for the request bound to the calling thread. */
  private final HttpServletRequest request;

  public HttpCatalogRequestContext(HttpServletRequest request) {
    this.request = request;
  }

  @Override
  public boolean columnDefaultPolicyBypass() {
    return header(TablesMvcConstants.HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  @Override
  public Optional<String> clientName() {
    return header(TablesMvcConstants.HTTP_HEADER_CLIENT_NAME);
  }

  @Override
  public Optional<String> userAgent() {
    return header(HttpHeaders.USER_AGENT);
  }

  /** Every value of the header, split at commas, so repeated headers add up. */
  @Override
  public Set<String> clientCapabilities() {
    if (RequestContextHolder.getRequestAttributes() == null) {
      return Collections.emptySet();
    }
    return Collections.list(request.getHeaders(TablesMvcConstants.HTTP_HEADER_CLIENT_CAPABILITIES))
        .stream()
        .flatMap(value -> Arrays.stream(value.split(",")))
        .map(capability -> capability.trim().toLowerCase(Locale.ROOT))
        .filter(capability -> !capability.isEmpty())
        .collect(Collectors.toSet());
  }

  /**
   * The current request's header, if it has one. Without a request bound to the thread the proxy
   * has no request to read, so there is no header.
   */
  private Optional<String> header(String name) {
    if (RequestContextHolder.getRequestAttributes() == null) {
      return Optional.empty();
    }
    return Optional.ofNullable(request.getHeader(name));
  }
}

package com.linkedin.openhouse.tables.config;

import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;

/**
 * Reads {@link CatalogRequestContext} options from the current request's headers. Outside a request
 * every option is off.
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

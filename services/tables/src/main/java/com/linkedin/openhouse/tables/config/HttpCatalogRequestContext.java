package com.linkedin.openhouse.tables.config;

import java.util.Optional;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * Reads {@link CatalogRequestContext} options from the current request's headers. Outside a request
 * every option is off.
 */
@Component
public class HttpCatalogRequestContext implements CatalogRequestContext {

  @Override
  public boolean columnDefaultPolicyBypass() {
    return header(TablesMvcConstants.HTTP_HEADER_COLUMN_DEFAULT_POLICY_BYPASS)
        .map(Boolean::parseBoolean)
        .orElse(false);
  }

  /**
   * The current request's header, if it has one. Spring MVC binds each request to its thread as
   * {@link ServletRequestAttributes}, the only {@link RequestAttributes} that carry headers, hence
   * the guarded cast; without such a request there is no header to read.
   */
  private static Optional<String> header(String name) {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    if (!(attributes instanceof ServletRequestAttributes)) {
      return Optional.empty();
    }
    return Optional.ofNullable(
        ((ServletRequestAttributes) attributes).getRequest().getHeader(name));
  }
}

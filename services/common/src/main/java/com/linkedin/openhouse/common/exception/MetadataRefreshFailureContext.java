package com.linkedin.openhouse.common.exception;

import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;

/** Identifies metadata-load failures without replacing their original exception types. */
public final class MetadataRefreshFailureContext {
  private static final String ATTRIBUTE = MetadataRefreshFailureContext.class.getName();

  private MetadataRefreshFailureContext() {}

  public static void mark(Throwable failure) {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    if (attributes != null) {
      attributes.setAttribute(ATTRIBUTE, failure, RequestAttributes.SCOPE_REQUEST);
    }
  }

  public static boolean matches(Throwable failure) {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    return attributes != null
        && attributes.getAttribute(ATTRIBUTE, RequestAttributes.SCOPE_REQUEST) == failure;
  }
}

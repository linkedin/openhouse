package com.linkedin.openhouse.common.utils;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/** The caller's system-action declaration; it never grants authorization. */
public final class SystemActionContext {
  public static final String HTTP_HEADER_SYSTEM_ACTION = "X-OpenHouse-System-Action";

  private SystemActionContext() {}

  /** Returns the raw declaration, or null without a Servlet request/header, without validation. */
  public static String getDeclaration() {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    return attributes instanceof ServletRequestAttributes
        ? ((ServletRequestAttributes) attributes).getRequest().getHeader(HTTP_HEADER_SYSTEM_ACTION)
        : null;
  }

  public static boolean isEnabled() {
    String declaration = getDeclaration();
    if (declaration == null || "false".equalsIgnoreCase(declaration)) {
      return false;
    }
    if ("true".equalsIgnoreCase(declaration)) {
      return true;
    }
    throw new RequestValidationFailureException(
        HTTP_HEADER_SYSTEM_ACTION + " must be true or false when supplied.");
  }
}

package com.linkedin.openhouse.common.utils;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import org.springframework.web.context.request.RequestAttributes;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/** The caller's action-type declaration; it never grants authorization. */
public final class ActionTypeContext {
  public static final String HTTP_HEADER_ACTION_TYPE = "X-OpenHouse-Action-Type";

  private ActionTypeContext() {}

  /** Returns the raw declaration, or null without a Servlet request/header, without validation. */
  public static String getDeclaration() {
    RequestAttributes attributes = RequestContextHolder.getRequestAttributes();
    return attributes instanceof ServletRequestAttributes
        ? ((ServletRequestAttributes) attributes).getRequest().getHeader(HTTP_HEADER_ACTION_TYPE)
        : null;
  }

  public static boolean isSystemAction() {
    String declaration = getDeclaration();
    if (declaration == null || "USER".equalsIgnoreCase(declaration)) {
      return false;
    }
    if ("SYSTEM".equalsIgnoreCase(declaration)) {
      return true;
    }
    throw new RequestValidationFailureException(
        HTTP_HEADER_ACTION_TYPE + " must be SYSTEM or USER when supplied.");
  }
}

package com.linkedin.openhouse.tables.readbridge;

import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

/**
 * The client catalog's {@code dangerously-bypass-column-default-policy} override, which the client
 * sends as a request header. It lifts column-default policy checks for that request only.
 */
public final class ColumnDefaultPolicyBypass {
  private static final String HEADER = "X-OpenHouse-Dangerously-Bypass-Column-Default-Policy";

  private ColumnDefaultPolicyBypass() {}

  /** Whether the current HTTP request carries the override. */
  public static boolean requested() {
    Object context = RequestContextHolder.getRequestAttributes();
    return context instanceof ServletRequestAttributes
        && "true"
            .equalsIgnoreCase(((ServletRequestAttributes) context).getRequest().getHeader(HEADER));
  }
}

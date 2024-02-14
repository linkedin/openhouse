package com.linkedin.openhouse.common.security;

import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.core.userdetails.User;

/** Utils class for authentication purpose. */
public final class AuthenticationUtils {
  private AuthenticationUtils() {}

  public static String extractAuthenticatedUserPrincipal() {
    SecurityContext securityContext = SecurityContextHolder.getContext();
    String authenticatedUserPrincipal = "undefined";
    if (securityContext != null
        && securityContext.getAuthentication() != null
        && securityContext.getAuthentication().getPrincipal() != null) {
      User user = (User) securityContext.getAuthentication().getPrincipal();
      authenticatedUserPrincipal = user.getUsername();
    }
    return authenticatedUserPrincipal;
  }
}

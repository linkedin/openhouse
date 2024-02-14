package com.linkedin.openhouse.tables.mock.controller;

import com.linkedin.openhouse.common.security.DummyAuthenticationContext;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.test.context.support.WithSecurityContextFactory;

public class MockUnauthenticatedSecurityContextFactory
    implements WithSecurityContextFactory<MockUnauthenticatedUser> {
  /**
   * Create a {@link SecurityContext} given an Annotation.
   *
   * @param annotation the {@link Annotation} to create the {@link SecurityContext} from. Cannot be
   *     null.
   * @return the {@link SecurityContext} to use. Cannot be null.
   */
  @Override
  public SecurityContext createSecurityContext(MockUnauthenticatedUser annotation) {
    SecurityContext context = SecurityContextHolder.createEmptyContext();
    context.setAuthentication(DummyAuthenticationContext.builder().isAuthenticated(false).build());
    return context;
  }
}

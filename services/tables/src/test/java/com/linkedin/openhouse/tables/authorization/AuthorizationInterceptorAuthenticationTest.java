package com.linkedin.openhouse.tables.authorization;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Method;
import java.util.Collections;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.jupiter.api.Test;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.User;

public class AuthorizationInterceptorAuthenticationTest {

  private final AuthorizationInterceptor interceptor = new AuthorizationInterceptor();

  @Test
  public void missingAuthenticationIsDeniedWithoutThrowing() throws NoSuchMethodException {
    AuthorizationDecision decision =
        interceptor.check(() -> null, invocation(SecuredEndpoints.authenticatedOnlyMethod()));

    assertFalse(decision.isGranted());
  }

  @Test
  public void missingSecurityContextIsDeniedWithoutThrowing() throws NoSuchMethodException {
    AuthorizationDecision decision =
        interceptor.check(
            () -> {
              throw new AuthenticationCredentialsNotFoundException("missing");
            },
            invocation(SecuredEndpoints.authenticatedOnlyMethod()));

    assertFalse(decision.isGranted());
  }

  @Test
  public void authenticatedSentinelGrantsAnyAuthenticatedPrincipal() throws NoSuchMethodException {
    AuthorizationDecision decision =
        interceptor.check(
            () -> authenticatedPrincipal("test-user"),
            invocation(SecuredEndpoints.authenticatedOnlyMethod()));

    assertTrue(
        decision.isGranted(),
        "AUTHENTICATED is a route guard only; it must not become a resource privilege check.");
  }

  private static Authentication authenticatedPrincipal(String username) {
    User principal = new User(username, "unused", true, true, true, true, Collections.emptyList());
    return new UsernamePasswordAuthenticationToken(principal, "unused", principal.getAuthorities());
  }

  private static MethodInvocation invocation(Method method) {
    MethodInvocation invocation = mock(MethodInvocation.class);
    when(invocation.getMethod()).thenReturn(method);
    when(invocation.getArguments()).thenReturn(new Object[0]);
    return invocation;
  }

  private static final class SecuredEndpoints {
    private SecuredEndpoints() {}

    @Secured(Privileges.Privilege.AUTHENTICATED)
    void authenticatedOnly() {}

    private static Method authenticatedOnlyMethod() throws NoSuchMethodException {
      return SecuredEndpoints.class.getDeclaredMethod("authenticatedOnly");
    }
  }
}

package com.linkedin.openhouse.tables.authorization;

import java.lang.annotation.Annotation;
import java.lang.reflect.Parameter;
import java.util.Arrays;
import java.util.Optional;
import java.util.function.Supplier;
import lombok.extern.slf4j.Slf4j;
import org.aopalliance.intercept.MethodInvocation;
import org.springframework.security.access.annotation.Secured;
import org.springframework.security.authentication.AuthenticationCredentialsNotFoundException;
import org.springframework.security.authorization.AuthorizationDecision;
import org.springframework.security.authorization.AuthorizationManager;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.userdetails.User;

/**
 * For each endpoint annotated with {@link Secured} annotation, check if the {@link Authentication}
 * user has permission to access the REST resources.
 */
@Slf4j
public class AuthorizationInterceptor implements AuthorizationManager<MethodInvocation> {

  private static final String DATABASE_ID = "databaseId";
  private static final String TABLE_ID = "tableId";

  /**
   * Determines if access is granted for a specific {@link Authentication} and {@link
   * MethodInvocation}.
   *
   * @param authenticationSupplier the {@link Supplier} of the {@link Authentication} to check
   * @param methodInvocation the {@link MethodInvocation} to check
   * @return an {@link AuthorizationDecision} to capture authorization decision.
   * @throws {@link org.springframework.security.access.AccessDeniedException} if authorization
   *     fails
   */
  @Override
  public AuthorizationDecision check(
      Supplier<Authentication> authenticationSupplier, MethodInvocation methodInvocation) {

    // Make sure the request is authenticated, if yes retrieve the user principal.
    Authentication authentication = null;
    try {
      authentication = authenticationSupplier.get();
    } catch (AuthenticationCredentialsNotFoundException exception) {
      return new AuthorizationDecision(false);
    }
    if (!authentication.isAuthenticated()) {
      return new AuthorizationDecision(false);
    }

    User user = (User) authentication.getPrincipal();
    String principal = user.getUsername();

    // From @Secured annotation retrieve privileges expected for the method call.
    Optional<Annotation> securedAnnotation =
        Arrays.stream(methodInvocation.getMethod().getAnnotations())
            .filter(annotation -> Secured.class.isAssignableFrom(annotation.annotationType()))
            .findFirst();
    if (!securedAnnotation.isPresent()) {
      // Nothing to check for.
      return new AuthorizationDecision(true);
    }
    String[] privileges = ((Secured) securedAnnotation.get()).value();

    // Now from the method parameters retrieve databaseId and tableId.
    String databaseId = null;
    String tableId = null;
    for (int idx = 0; idx < methodInvocation.getMethod().getParameters().length; ++idx) {
      Parameter parameter = methodInvocation.getMethod().getParameters()[idx];
      if (DATABASE_ID.equals(parameter.getName())) {
        databaseId = (String) methodInvocation.getArguments()[idx];
      } else if (TABLE_ID.equals(parameter.getName())) {
        tableId = (String) methodInvocation.getArguments()[idx];
      }
    }

    // Next PR validate that privilege check has sufficient valid inputs. For example,
    // GET_TABLE privilege has NonNull databaseId and tableId. Or, SHOW_TABLES privilege has
    // NonNull databaseId and Null tableId.
    // Next PR will have code to check access for principal on resources.
    log.info(
        "Checking privileges: [{}] for principal: {} on databaseId: {} and tableId: {}",
        String.join(",", privileges),
        principal,
        databaseId,
        tableId);
    return new AuthorizationDecision(true);
  }
}

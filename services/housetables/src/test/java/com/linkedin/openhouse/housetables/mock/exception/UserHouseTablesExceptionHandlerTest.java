package com.linkedin.openhouse.housetables.mock.exception;

import static org.assertj.core.api.Assertions.assertThat;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.AlreadyExistsException;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.NoSuchEntityException;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.housetables.controller.JobTablesController;
import com.linkedin.openhouse.housetables.controller.ToggleStatusesController;
import com.linkedin.openhouse.housetables.controller.UserHouseTablesController;
import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeConversionException;
import com.linkedin.openhouse.housetables.exception.CorruptUserTableDataException;
import com.linkedin.openhouse.housetables.exception.UserTablePersistenceException;
import com.linkedin.openhouse.housetables.exception.UserTableReadException;
import com.linkedin.openhouse.housetables.exception.handler.UserHouseTablesExceptionHandler;
import io.swagger.v3.oas.annotations.Hidden;
import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.persistence.PersistenceException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.method.ControllerAdviceBean;
import org.springframework.web.method.annotation.ExceptionHandlerMethodResolver;

/**
 * Composition-based on purpose: with three declared mappings and no inheritance, anything unnamed
 * falls through to {@link OpenHouseExceptionHandler} despite the highest precedence here.
 */
public class UserHouseTablesExceptionHandlerTest {

  private static final String CORRUPT_MSG =
      "Column user_table_row.entity_type holds unrecognized value ['UNKNOWN']; "
          + "only TABLE, VIEW (in any case) and NULL are valid";

  private static final String HIBERNATE_MSG = "Error attempting to apply AttributeConverter";

  private final UserHouseTablesExceptionHandler handler = new UserHouseTablesExceptionHandler();

  private final ExceptionHandlerMethodResolver resolver =
      new ExceptionHandlerMethodResolver(UserHouseTablesExceptionHandler.class);

  private static CorruptEntityTypeConversionException corruption() {
    return new CorruptEntityTypeConversionException(
        CORRUPT_MSG, new IllegalArgumentException("UNKNOWN"));
  }

  private static List<Method> declaredExceptionHandlers() {
    return Arrays.stream(UserHouseTablesExceptionHandler.class.getDeclaredMethods())
        .filter(method -> method.isAnnotationPresent(ExceptionHandler.class))
        .collect(Collectors.toList());
  }

  // -------------------------------------------------------------------------------------------
  // shape of the advice
  // -------------------------------------------------------------------------------------------

  /** Three mappings and no more; a fourth would silently take responsibility for something. */
  @Test
  public void testAdviceDeclaresExactlyTheThreeIntendedMappings() {
    List<Class<?>[]> mapped =
        declaredExceptionHandlers().stream()
            .map(method -> method.getAnnotation(ExceptionHandler.class).value())
            .collect(Collectors.toList());

    Assertions.assertEquals(3, mapped.size());
    assertThat(mapped.stream().flatMap(Arrays::stream).collect(Collectors.toList()))
        .containsExactlyInAnyOrder(
            UserTablePersistenceException.class,
            CorruptEntityTypeConversionException.class,
            JpaSystemException.class,
            InvalidDataAccessApiUsageException.class);
  }

  /**
   * Without {@link Hidden}, springdoc adds these responses to every scoped controller operation.
   */
  @Test
  public void testEveryHandlerMethodIsHiddenFromTheGeneratedSpec() {
    assertThat(declaredExceptionHandlers())
        .isNotEmpty()
        .allSatisfy(method -> assertThat(method.isAnnotationPresent(Hidden.class)).isTrue());
  }

  /**
   * Inheriting would register the parent's {@code Exception.class} mapping through {@code
   * MethodIntrospector}, swallowing every 400/404/409 the shared advice owns.
   */
  @Test
  public void testAdviceDoesNotInheritTheSharedAdvice() {
    Assertions.assertFalse(
        OpenHouseExceptionHandler.class.isAssignableFrom(UserHouseTablesExceptionHandler.class),
        "inheriting the shared advice would register its Exception.class mapping here");
    Assertions.assertEquals(Object.class, UserHouseTablesExceptionHandler.class.getSuperclass());
  }

  @Test
  public void testAdviceAppliesOnlyToTheUserHouseTablesController() {
    ControllerAdviceBean advice = new ControllerAdviceBean(handler);

    assertThat(advice.isApplicableToBeanType(UserHouseTablesController.class)).isTrue();
    assertThat(advice.isApplicableToBeanType(JobTablesController.class)).isFalse();
    assertThat(advice.isApplicableToBeanType(ToggleStatusesController.class)).isFalse();
  }

  /** The merged annotation is what Spring reads for scoping. */
  @Test
  public void testAdviceIsScopedAndRunsAtHighestPrecedence() {
    ControllerAdvice merged =
        AnnotatedElementUtils.findMergedAnnotation(
            UserHouseTablesExceptionHandler.class, ControllerAdvice.class);

    Assertions.assertNotNull(merged);
    assertThat(merged.assignableTypes()).containsExactly(UserHouseTablesController.class);
    Assertions.assertEquals(
        Ordered.HIGHEST_PRECEDENCE, new ControllerAdviceBean(handler).getOrder());
  }

  // -------------------------------------------------------------------------------------------
  // what the resolver does and does not claim
  // -------------------------------------------------------------------------------------------

  static Stream<Class<? extends Throwable>> claimedExceptions() {
    return Stream.of(
        UserTablePersistenceException.class,
        UserTableReadException.class,
        CorruptUserTableDataException.class,
        CorruptEntityTypeConversionException.class,
        JpaSystemException.class,
        InvalidDataAccessApiUsageException.class);
  }

  @ParameterizedTest
  @MethodSource("claimedExceptions")
  public void testResolverClaimsEveryModuleAndWrapperFailure(Class<? extends Throwable> claimed) {
    assertThat(resolver.resolveMethodByExceptionType(claimed))
        .as("scoped advice must handle %s", claimed.getSimpleName())
        .isNotNull();
  }

  static Stream<Class<? extends Throwable>> fallThroughExceptions() {
    return Stream.of(
        RequestValidationFailureException.class,
        NoSuchUserTableException.class,
        NoSuchEntityException.class,
        AlreadyExistsException.class,
        EntityConcurrentModificationException.class,
        DataIntegrityViolationException.class,
        CannotAcquireLockException.class,
        IllegalArgumentException.class,
        IllegalStateException.class,
        RuntimeException.class,
        Exception.class);
  }

  /** Resolving to nothing here is what lets Spring advance to the shared advice. */
  @ParameterizedTest
  @MethodSource("fallThroughExceptions")
  public void testResolverClaimsNothingTheSharedAdviceOwns(Class<? extends Throwable> fallThrough) {
    assertThat(resolver.resolveMethodByExceptionType(fallThrough))
        .as("%s must fall through to the shared advice", fallThrough.getSimpleName())
        .isNull();
    assertThat(
            new ExceptionHandlerMethodResolver(OpenHouseExceptionHandler.class)
                .resolveMethodByExceptionType(fallThrough))
        .as("the shared advice is what answers %s", fallThrough.getSimpleName())
        .isNotNull();
  }

  // -------------------------------------------------------------------------------------------
  // rendered bodies
  // -------------------------------------------------------------------------------------------

  /** A server-state failure whatever wrote it, so a 500 with the diagnostic rather than a 400. */
  @Test
  public void testModuleFailureCarryingCorruptionRendersTheColumnDiagnostic() {
    ResponseEntity<ErrorResponseBody> response =
        handler.handleUserTablePersistenceException(
            new CorruptUserTableDataException(
                "read failed",
                new JpaSystemException(new PersistenceException(HIBERNATE_MSG, corruption()))));

    assertServerErrorCarryingDiagnostic(response);
    Assertions.assertEquals("UNKNOWN", response.getBody().getCause());
    Assertions.assertFalse(response.getBody().getMessage().contains(HIBERNATE_MSG));
  }

  @Test
  public void testDirectConverterEscapeRendersTheColumnDiagnostic() {
    assertServerErrorCarryingDiagnostic(
        handler.handleCorruptEntityTypeConversionException(corruption()));
  }

  /** The compatibility branch for the frozen table reads that still leak a raw wrapper. */
  @Test
  public void testRawWrapperCarryingCorruptionRendersTheColumnDiagnostic() {
    assertServerErrorCarryingDiagnostic(
        handler.handleRawPersistenceWrapper(
            new JpaSystemException(new PersistenceException(HIBERNATE_MSG, corruption()))));
    assertServerErrorCarryingDiagnostic(
        handler.handleRawPersistenceWrapper(
            new InvalidDataAccessApiUsageException(HIBERNATE_MSG, corruption())));
  }

  @Test
  public void testDeeplyNestedCorruptionIsStillUnwrapped() {
    assertServerErrorCarryingDiagnostic(
        handler.handleRawPersistenceWrapper(
            new JpaSystemException(
                new PersistenceException(
                    HIBERNATE_MSG,
                    new IllegalStateException(
                        "outer", new RuntimeException("inner", corruption()))))));
  }

  /** Renders the preserved original cause, never a corruption diagnostic it does not have. */
  @Test
  public void testModuleFailureWithoutCorruptionRendersGenerically() {
    JpaSystemException original =
        new JpaSystemException(new PersistenceException("connection reset"));

    ResponseEntity<ErrorResponseBody> response =
        handler.handleUserTablePersistenceException(
            new UserTableReadException("read failed", original));

    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getBody().getStatus());
    Assertions.assertFalse(response.getBody().getMessage().contains(CORRUPT_MSG));
    Assertions.assertTrue(response.getBody().getMessage().contains("connection reset"));
    Assertions.assertNotNull(response.getBody().getStacktrace());
  }

  @Test
  public void testUnrelatedRawWrapperRendersGenerically() {
    JpaSystemException unrelated =
        new JpaSystemException(new PersistenceException("connection reset"));

    ResponseEntity<ErrorResponseBody> response = handler.handleRawPersistenceWrapper(unrelated);

    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    Assertions.assertEquals(unrelated.toString(), response.getBody().getMessage());
    Assertions.assertFalse(response.getBody().getMessage().contains(CORRUPT_MSG));
  }

  @Test
  public void testCyclicCauseChainTerminates() {
    ResponseEntity<ErrorResponseBody> response =
        handler.handleRawPersistenceWrapper(
            new JpaSystemException(new SelfCausedException("cycle")));

    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    Assertions.assertFalse(response.getBody().getMessage().contains(CORRUPT_MSG));
  }

  private void assertServerErrorCarryingDiagnostic(ResponseEntity<ErrorResponseBody> response) {
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
    ErrorResponseBody body = response.getBody();
    Assertions.assertNotNull(body);
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, body.getStatus());
    Assertions.assertEquals(HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase(), body.getError());
    Assertions.assertEquals(CORRUPT_MSG, body.getMessage());
    Assertions.assertNotNull(body.getStacktrace());
    Assertions.assertTrue(
        body.getStacktrace().contains(CorruptEntityTypeConversionException.class.getSimpleName()));
  }

  /**
   * {@link Throwable#initCause} forbids a self-referential cause, so the cycle is expressed by
   * overriding the accessor.
   */
  private static class SelfCausedException extends RuntimeException {
    SelfCausedException(String message) {
      super(message);
    }

    @Override
    public synchronized Throwable getCause() {
      return this;
    }
  }
}

package com.linkedin.openhouse.housetables.exception.handler;

import com.linkedin.openhouse.common.api.spec.ErrorResponseBody;
import com.linkedin.openhouse.common.exception.handler.ErrorResponseBodyFactory;
import com.linkedin.openhouse.common.exception.handler.OpenHouseExceptionHandler;
import com.linkedin.openhouse.housetables.controller.UserHouseTablesController;
import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeCauseFinder;
import com.linkedin.openhouse.housetables.exception.CorruptEntityTypeConversionException;
import com.linkedin.openhouse.housetables.exception.UserTablePersistenceException;
import io.swagger.v3.oas.annotations.Hidden;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.dao.InvalidDataAccessApiUsageException;
import org.springframework.dao.NonTransientDataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.orm.jpa.JpaSystemException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 * House Tables' own translation of persistence failures, scoped to {@link
 * UserHouseTablesController} so no other service inherits ORM knowledge.
 *
 * <p>Composition, not inheritance: it deliberately does not extend {@link
 * OpenHouseExceptionHandler}, because {@code ExceptionHandlerMethodResolver} registers every
 * inherited {@code @ExceptionHandler} and a subclass would therefore inherit the parent's {@code
 * Exception.class} mapping and become total for this controller. With only the three mappings below
 * declared, everything else resolves to nothing here and Spring advances to the shared advice —
 * even though this advice runs at the highest precedence.
 *
 * <p>Every method is {@link Hidden}: springdoc otherwise adds their responses to every operation on
 * the scoped controller and changes the generated client contract.
 */
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
@RestControllerAdvice(assignableTypes = UserHouseTablesController.class)
public class UserHouseTablesExceptionHandler {

  private final ErrorResponseBodyFactory errorResponseBodyFactory = new ErrorResponseBodyFactory();

  /**
   * The module's own unchecked persistence failures, corrupt data included. A failure carrying
   * corruption renders the converter's column-and-value diagnostic; anything else renders its
   * preserved original cause generically.
   */
  @Hidden
  @ExceptionHandler(UserTablePersistenceException.class)
  public ResponseEntity<ErrorResponseBody> handleUserTablePersistenceException(
      UserTablePersistenceException userTablePersistenceException) {
    return render(userTablePersistenceException, originalCauseOf(userTablePersistenceException));
  }

  /** Defense in depth for a converter escape that reached the controller untranslated. */
  @Hidden
  @ExceptionHandler(CorruptEntityTypeConversionException.class)
  public ResponseEntity<ErrorResponseBody> handleCorruptEntityTypeConversionException(
      CorruptEntityTypeConversionException corruptEntityTypeConversionException) {
    return render(corruptEntityTypeConversionException, corruptEntityTypeConversionException);
  }

  /**
   * Compatibility only, for the frozen table read paths that still expose raw wrappers. New neutral
   * and view reads are already translated by {@code JpaUserTableReadRepository}, so they never
   * arrive here.
   */
  @Hidden
  @ExceptionHandler({JpaSystemException.class, InvalidDataAccessApiUsageException.class})
  public ResponseEntity<ErrorResponseBody> handleRawPersistenceWrapper(
      NonTransientDataAccessException dataAccessException) {
    return render(dataAccessException, dataAccessException);
  }

  /**
   * Corruption anywhere in the chain is rendered from the converter exception itself, so the
   * offending column and value survive the persistence wrapping. Anything else is rendered
   * generically from the failure that actually occurred.
   */
  private ResponseEntity<ErrorResponseBody> render(
      Throwable received, Throwable genericRenderingSource) {
    ErrorResponseBody body =
        CorruptEntityTypeCauseFinder.find(received)
            .map(
                corruption -> {
                  log.error("Corrupt entity type read from storage:\n", received);
                  return errorResponseBodyFactory.build(
                      HttpStatus.INTERNAL_SERVER_ERROR, corruption.getMessage(), corruption);
                })
            .orElseGet(() -> errorResponseBodyFactory.genericServerError(genericRenderingSource));
    return new ResponseEntity<>(body, body.getStatus());
  }

  /** A module failure preserves the original persistence failure; that is what is rendered. */
  private static Throwable originalCauseOf(UserTablePersistenceException moduleFailure) {
    return moduleFailure.getCause() == null ? moduleFailure : moduleFailure.getCause();
  }
}

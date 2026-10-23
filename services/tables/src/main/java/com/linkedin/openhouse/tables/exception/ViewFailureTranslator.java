package com.linkedin.openhouse.tables.exception;

import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableNotFoundException;
import com.linkedin.openhouse.internal.catalog.repository.exception.HouseTableRepositoryStateUnknownException;
import com.linkedin.openhouse.internal.catalog.view.ViewNameOccupiedException;
import com.linkedin.openhouse.internal.catalog.view.ViewSourceDialectMismatchException;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.security.access.AuthorizationServiceException;

/** Maps failures raised while serving a view operation to the typed {@link ViewApiException}s. */
public final class ViewFailureTranslator {

  private ViewFailureTranslator() {}

  /**
   * Write-failure dispatch: an ambiguous commit becomes {@link ViewErrorCode#COMMIT_STATE_UNKNOWN};
   * any other failure is rethrown as a {@link ViewApiException}, except authorization failures,
   * which keep their own type for the dedicated advice.
   */
  public static RuntimeException forWrite(RuntimeException e) {
    if (e instanceof CommitStateUnknownException) {
      return new ViewApiException(
          ViewErrorCode.COMMIT_STATE_UNKNOWN, "View commit outcome unknown", e);
    }
    if (e instanceof ViewApiException
        || e instanceof AccessDeniedException
        || e instanceof AuthorizationServiceException) {
      return e;
    }
    if (e instanceof ViewSourceDialectMismatchException) {
      return new ViewApiException(
          ViewErrorCode.INVALID_VIEW_DEFINITION, "sourceDialect must match the current view", e);
    }
    if (e instanceof AlreadyExistsException) {
      return new ViewApiException(ViewErrorCode.VIEW_ALREADY_EXISTS, "View already exists", e);
    }
    if (e instanceof ViewNameOccupiedException) {
      return new ViewApiException(
          ViewErrorCode.NAME_ALREADY_EXISTS_AS_TABLE, "Name already exists as a table", e);
    }
    if (e instanceof CommitFailedException) {
      return new ViewApiException(
          ViewErrorCode.CONCURRENT_VIEW_MODIFICATION, "Concurrent view modification", e);
    }
    if (e instanceof HouseTableRepositoryStateUnknownException) {
      // A known precommit transient (e.g. a capture-time HTS outage), distinct from an
      // unacknowledged publication (CommitStateUnknownException, handled separately above):
      // driven by exception type only, never by matching a message.
      return new ViewApiException(
          ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, "View service unavailable", e);
    }
    // Other engine BadRequestException (trusted server input), HouseTableCallerException,
    // corrupt-row IllegalStateException, and unexpected failures remain server faults.
    return new ViewApiException(
        ViewErrorCode.INTERNAL_VIEW_ERROR, "Unexpected view service failure", e);
  }

  /** Read-failure dispatch for failures that are not already a {@link ViewApiException}. */
  public static ViewApiException forRead(RuntimeException e) {
    if (e instanceof HouseTableNotFoundException) {
      return new ViewApiException(ViewErrorCode.NO_SUCH_VIEW, "No such view", e);
    }
    if (e instanceof HouseTableRepositoryStateUnknownException) {
      return new ViewApiException(
          ViewErrorCode.VIEW_SERVICE_UNAVAILABLE, "View service unavailable", e);
    }
    // HouseTableCallerException (server-side 4xx to a trusted call), corrupt-row
    // IllegalStateException, and any other unexpected read failure are server faults.
    return new ViewApiException(
        ViewErrorCode.INTERNAL_VIEW_ERROR, "Unexpected view service failure", e);
  }
}

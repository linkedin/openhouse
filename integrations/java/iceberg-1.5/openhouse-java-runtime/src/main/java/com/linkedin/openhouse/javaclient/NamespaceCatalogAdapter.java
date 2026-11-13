package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.javaclient.exception.AuthenticationRequiredException;
import com.linkedin.openhouse.javaclient.exception.CatalogOperationException;
import com.linkedin.openhouse.javaclient.exception.DuplicateCreationException;
import com.linkedin.openhouse.javaclient.exception.InvalidRequestException;
import com.linkedin.openhouse.javaclient.exception.MutationUncertainException;
import com.linkedin.openhouse.javaclient.exception.NotEmptyException;
import com.linkedin.openhouse.javaclient.exception.NotFoundException;
import com.linkedin.openhouse.javaclient.exception.PermissionDeniedException;
import com.linkedin.openhouse.javaclient.exception.VersionConflictException;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NotAuthorizedException;
import org.apache.iceberg.exceptions.RESTException;
import org.apache.iceberg.exceptions.ValidationException;

/** Adapts the shared checked namespace contract to Iceberg's runtime-only interface. */
final class NamespaceCatalogAdapter {
  @FunctionalInterface
  interface CheckedCall<T> {
    T execute() throws CatalogOperationException;
  }

  private NamespaceCatalogAdapter() {}

  static <T> T execute(CheckedCall<T> call) {
    try {
      return call.execute();
    } catch (InvalidRequestException failure) {
      throw new ValidationException(failure, "%s", failure.getMessage());
    } catch (AuthenticationRequiredException failure) {
      throw new NotAuthorizedException(failure, "Namespace authentication required");
    } catch (PermissionDeniedException failure) {
      throw new ForbiddenException(failure, "Namespace access denied");
    } catch (NotFoundException failure) {
      throw new NoSuchNamespaceException(failure, "Namespace metadata does not exist");
    } catch (DuplicateCreationException failure) {
      throw new AlreadyExistsException(failure, "Namespace already exists");
    } catch (NotEmptyException failure) {
      throw new NamespaceNotEmptyException(failure, "Namespace contains tables or views");
    } catch (VersionConflictException failure) {
      throw new CommitFailedException(failure, "Namespace metadata changed concurrently");
    } catch (MutationUncertainException failure) {
      throw new CommitStateUnknownException(failure);
    } catch (CatalogOperationException failure) {
      throw new RESTException(failure, "Namespace request failed");
    }
  }
}

package com.linkedin.openhouse.javaclient;

import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataAlreadyExistsException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataConcurrentModificationException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataForbiddenException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataInvalidRequestException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataMutationUncertainException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataNotEmptyException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataNotFoundException;
import com.linkedin.openhouse.javaclient.exception.NamespaceMetadataUnauthenticatedException;
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
    T execute() throws NamespaceMetadataException;
  }

  private NamespaceCatalogAdapter() {}

  static <T> T execute(CheckedCall<T> call) {
    try {
      return call.execute();
    } catch (NamespaceMetadataInvalidRequestException failure) {
      throw new ValidationException(failure, "%s", failure.getMessage());
    } catch (NamespaceMetadataUnauthenticatedException failure) {
      throw new NotAuthorizedException(failure, "Namespace authentication required");
    } catch (NamespaceMetadataForbiddenException failure) {
      throw new ForbiddenException(failure, "Namespace access denied");
    } catch (NamespaceMetadataNotFoundException failure) {
      throw new NoSuchNamespaceException(failure, "Namespace metadata does not exist");
    } catch (NamespaceMetadataAlreadyExistsException failure) {
      throw new AlreadyExistsException(failure, "Namespace already exists");
    } catch (NamespaceMetadataNotEmptyException failure) {
      throw new NamespaceNotEmptyException(failure, "Namespace contains tables or views");
    } catch (NamespaceMetadataConcurrentModificationException failure) {
      throw new CommitFailedException(failure, "Namespace metadata changed concurrently");
    } catch (NamespaceMetadataMutationUncertainException failure) {
      throw new CommitStateUnknownException(failure);
    } catch (NamespaceMetadataException failure) {
      throw new RESTException(failure, "Namespace request failed");
    }
  }
}

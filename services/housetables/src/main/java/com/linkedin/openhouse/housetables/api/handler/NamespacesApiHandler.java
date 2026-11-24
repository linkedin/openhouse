package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;

/** Operations on namespaces stored in HouseTables. */
public interface NamespacesApiHandler {
  /**
   * Creates a namespace.
   *
   * @throws DuplicateCreationException if the namespace name already exists
   */
  ApiResponse<NamespaceMetadataResponse> createNamespace(CreateNamespaceRequestBody request)
      throws DuplicateCreationException;

  /**
   * Loads a namespace.
   *
   * @throws NotFoundException if the namespace does not exist
   */
  ApiResponse<NamespaceMetadataResponse> getNamespace(String namespaceName)
      throws NotFoundException;

  /**
   * Deletes a namespace.
   *
   * @throws NotFoundException if the namespace does not exist
   * @throws EntityConcurrentModificationException if a concurrent write wins
   */
  ApiResponse<Void> deleteNamespace(String namespaceName) throws NotFoundException;

  /**
   * Replaces a namespace if its identity and revision still match the stored namespace.
   *
   * @throws NotFoundException if the namespace does not exist
   * @throws EntityConcurrentModificationException if the namespace is stale or a concurrent write
   *     wins
   */
  ApiResponse<NamespaceMetadataResponse> updateNamespace(
      String namespaceName, UpdateNamespaceRequestBody request) throws NotFoundException;
}

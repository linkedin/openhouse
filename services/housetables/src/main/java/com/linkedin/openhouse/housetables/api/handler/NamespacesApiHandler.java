package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;

/** Operations on namespace documents stored in HouseTables. */
public interface NamespacesApiHandler {
  /**
   * Creates a document.
   *
   * @throws DuplicateCreationException if the namespace name already exists
   */
  ApiResponse<NamespaceMetadataResponse> createNamespace(CreateNamespaceRequestBody request)
      throws DuplicateCreationException;

  /**
   * Loads a document.
   *
   * @throws NotFoundException if the namespace does not exist
   */
  ApiResponse<NamespaceMetadataResponse> getNamespace(String namespaceName)
      throws NotFoundException;

  /**
   * Deletes a document.
   *
   * @throws NotFoundException if the namespace does not exist
   * @throws EntityConcurrentModificationException if a concurrent write wins
   */
  ApiResponse<Void> deleteNamespace(String namespaceName) throws NotFoundException;

  /**
   * Replaces a document if its identity and revision still match the stored document.
   *
   * @throws NotFoundException if the namespace does not exist
   * @throws EntityConcurrentModificationException if the document is stale or a concurrent write
   *     wins
   */
  ApiResponse<NamespaceMetadataResponse> updateNamespace(
      String namespaceName, UpdateNamespaceRequestBody request) throws NotFoundException;
}

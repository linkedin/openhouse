package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.InvalidPropertyUpdateException;
import com.linkedin.openhouse.common.exception.checked.InvalidRequestException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;

public interface NamespacesApiHandler {
  ApiResponse<NamespaceMetadataResponse> createNamespace(CreateNamespaceRequestBody request)
      throws InvalidRequestException, DuplicateCreationException;

  ApiResponse<NamespaceMetadataResponse> getNamespace(String namespaceName)
      throws InvalidRequestException, NotFoundException;

  ApiResponse<Void> deleteNamespace(String namespaceName)
      throws InvalidRequestException, NotFoundException;

  ApiResponse<UpdateNamespacePropertiesResponseBody> updateNamespaceProperties(
      String namespaceName, UpdateNamespacePropertiesRequestBody request)
      throws InvalidRequestException, NotFoundException, InvalidPropertyUpdateException;
}

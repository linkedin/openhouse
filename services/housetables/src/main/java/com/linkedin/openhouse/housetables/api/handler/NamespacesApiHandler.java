package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataException;

public interface NamespacesApiHandler {
  ApiResponse<NamespaceMetadataResponse> createNamespace(CreateNamespaceRequestBody request)
      throws NamespaceMetadataException;

  ApiResponse<NamespaceMetadataResponse> getNamespace(String namespaceName)
      throws NamespaceMetadataException;

  ApiResponse<Void> deleteNamespace(String namespaceName) throws NamespaceMetadataException;

  ApiResponse<UpdateNamespacePropertiesResponseBody> updateNamespaceProperties(
      String namespaceName, UpdateNamespacePropertiesRequestBody request)
      throws NamespaceMetadataException;
}

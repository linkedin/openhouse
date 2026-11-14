package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.housetables.services.NamespacesService;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseNamespacesApiHandler implements NamespacesApiHandler {
  private final NamespacesService namespacesService;

  public OpenHouseNamespacesApiHandler(NamespacesService namespacesService) {
    this.namespacesService = namespacesService;
  }

  @Override
  public ApiResponse<NamespaceMetadataResponse> createNamespace(CreateNamespaceRequestBody request)
      throws DuplicateCreationException {
    NamespaceMetadata metadata =
        namespacesService.createNamespace(request.getNamespaceName(), request.getProperties());
    return ApiResponse.<NamespaceMetadataResponse>builder()
        .httpStatus(HttpStatus.CREATED)
        .responseBody(
            NamespaceMetadataResponse.builder()
                .namespaceUuid(metadata.getNamespaceUuid())
                .namespaceName(metadata.getNamespaceName())
                .properties(metadata.getProperties())
                .build())
        .build();
  }

  @Override
  public ApiResponse<NamespaceMetadataResponse> getNamespace(String namespaceName)
      throws NotFoundException {
    NamespaceMetadata metadata = namespacesService.getNamespace(namespaceName);
    return ApiResponse.<NamespaceMetadataResponse>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            NamespaceMetadataResponse.builder()
                .namespaceUuid(metadata.getNamespaceUuid())
                .namespaceName(metadata.getNamespaceName())
                .properties(metadata.getProperties())
                .build())
        .build();
  }

  @Override
  public ApiResponse<Void> deleteNamespace(String namespaceName) throws NotFoundException {
    namespacesService.deleteNamespace(namespaceName);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<UpdateNamespacePropertiesResponseBody> updateNamespaceProperties(
      String namespaceName, UpdateNamespacePropertiesRequestBody request) throws NotFoundException {
    NamespacePropertyUpdate result =
        namespacesService.updateNamespaceProperties(
            namespaceName, request.getUpdates(), request.getRemovals());
    return ApiResponse.<UpdateNamespacePropertiesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            UpdateNamespacePropertiesResponseBody.builder()
                .updated(result.getUpdated())
                .removed(result.getRemoved())
                .missing(result.getMissing())
                .build())
        .build();
  }
}

package com.linkedin.openhouse.housetables.api.handler;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.checked.DuplicateCreationException;
import com.linkedin.openhouse.common.exception.checked.NotFoundException;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
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
                .revision(metadata.getRevision())
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
                .revision(metadata.getRevision())
                .build())
        .build();
  }

  @Override
  public ApiResponse<Void> deleteNamespace(String namespaceName) throws NotFoundException {
    namespacesService.deleteNamespace(namespaceName);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<NamespaceMetadataResponse> updateNamespace(
      String namespaceName, UpdateNamespaceRequestBody request) throws NotFoundException {
    NamespaceMetadata result =
        namespacesService.updateNamespace(
            namespaceName,
            NamespaceMetadata.builder()
                .namespaceUuid(request.getNamespaceUuid())
                .namespaceName(request.getNamespaceName())
                .properties(request.getProperties())
                .revision(request.getRevision())
                .build());
    return ApiResponse.<NamespaceMetadataResponse>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            NamespaceMetadataResponse.builder()
                .namespaceUuid(result.getNamespaceUuid())
                .namespaceName(result.getNamespaceName())
                .properties(result.getProperties())
                .revision(result.getRevision())
                .build())
        .build();
  }
}

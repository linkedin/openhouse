package com.linkedin.openhouse.housetables.api.handler;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_REGEX;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
import com.linkedin.openhouse.housetables.services.InvalidNamespaceRequestException;
import com.linkedin.openhouse.housetables.services.NamespaceMetadataException;
import com.linkedin.openhouse.housetables.services.NamespacesService;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseNamespacesApiHandler implements NamespacesApiHandler {
  private final NamespacesService namespacesService;
  private final Validator validator;

  public OpenHouseNamespacesApiHandler(NamespacesService namespacesService, Validator validator) {
    this.namespacesService = namespacesService;
    this.validator = validator;
  }

  @Override
  public ApiResponse<NamespaceMetadataResponse> createNamespace(CreateNamespaceRequestBody request)
      throws NamespaceMetadataException {
    validateRequest(request);
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
      throws NamespaceMetadataException {
    validateNamespaceName(namespaceName);
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
  public ApiResponse<Void> deleteNamespace(String namespaceName) throws NamespaceMetadataException {
    validateNamespaceName(namespaceName);
    namespacesService.deleteNamespace(namespaceName);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<UpdateNamespacePropertiesResponseBody> updateNamespaceProperties(
      String namespaceName, UpdateNamespacePropertiesRequestBody request)
      throws NamespaceMetadataException {
    validateNamespaceName(namespaceName);
    validateRequest(request);
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

  private void validateNamespaceName(String namespaceName) throws InvalidNamespaceRequestException {
    if (namespaceName.isEmpty()
        || namespaceName.length() > 128
        || !namespaceName.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      throw new InvalidNamespaceRequestException(
          "Namespace name must contain 1 to 128 letters, digits, or underscores.");
    }
  }

  private <T> void validateRequest(T request) throws InvalidNamespaceRequestException {
    Set<ConstraintViolation<T>> violations = validator.validate(request);
    if (!violations.isEmpty()) {
      throw new InvalidNamespaceRequestException(
          violations.stream()
              .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
              .sorted()
              .collect(Collectors.joining("; ")));
    }
  }
}

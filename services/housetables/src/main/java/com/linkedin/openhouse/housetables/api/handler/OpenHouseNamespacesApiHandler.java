package com.linkedin.openhouse.housetables.api.handler;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_REGEX;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.common.exception.checked.DomainException;
import com.linkedin.openhouse.common.exception.checked.InvalidRequestException;
import com.linkedin.openhouse.housetables.api.spec.request.CreateNamespaceRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateNamespacePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.NamespaceMetadataResponse;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateNamespacePropertiesResponseBody;
import com.linkedin.openhouse.housetables.model.NamespaceMetadata;
import com.linkedin.openhouse.housetables.model.NamespacePropertyUpdate;
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
      throws DomainException {
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
      throws DomainException {
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
  public ApiResponse<Void> deleteNamespace(String namespaceName) throws DomainException {
    validateNamespaceName(namespaceName);
    namespacesService.deleteNamespace(namespaceName);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<UpdateNamespacePropertiesResponseBody> updateNamespaceProperties(
      String namespaceName, UpdateNamespacePropertiesRequestBody request) throws DomainException {
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

  private void validateNamespaceName(String namespaceName) throws InvalidRequestException {
    if (namespaceName.isEmpty()
        || namespaceName.length() > 128
        || !namespaceName.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      throw new InvalidRequestException(
          String.format(
              "Namespace name '%s' must contain 1 to 128 letters, digits, or underscores.",
              namespaceName));
    }
  }

  private <T> void validateRequest(T request) throws InvalidRequestException {
    Set<ConstraintViolation<T>> violations = validator.validate(request);
    if (!violations.isEmpty()) {
      /*
       * Field paths identify invalid inputs, and sorting makes the combined message deterministic.
       * Namespace values aid diagnosis; property values stay out of validation messages.
       */
      String validationMessage =
          violations.stream()
              .map(
                  violation ->
                      violation.getPropertyPath()
                          + ("namespaceName".equals(violation.getPropertyPath().toString())
                              ? " '" + violation.getInvalidValue() + "'"
                              : "")
                          + ": "
                          + violation.getMessage())
              .sorted()
              .collect(Collectors.joining("; "));
      throw new InvalidRequestException(validationMessage);
    }
  }
}

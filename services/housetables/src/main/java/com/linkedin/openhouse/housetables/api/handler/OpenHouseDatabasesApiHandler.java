package com.linkedin.openhouse.housetables.api.handler;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_REGEX;
import static com.linkedin.openhouse.housetables.services.DatabaseMetadataException.Reason.INVALID_REQUEST;

import com.linkedin.openhouse.common.api.spec.ApiResponse;
import com.linkedin.openhouse.housetables.api.spec.model.DatabaseMetadata;
import com.linkedin.openhouse.housetables.api.spec.request.CreateDatabaseRequestBody;
import com.linkedin.openhouse.housetables.api.spec.request.UpdateDatabasePropertiesRequestBody;
import com.linkedin.openhouse.housetables.api.spec.response.UpdateDatabasePropertiesResponseBody;
import com.linkedin.openhouse.housetables.model.DatabasePropertyUpdate;
import com.linkedin.openhouse.housetables.services.DatabaseMetadataException;
import com.linkedin.openhouse.housetables.services.DatabasesService;
import java.util.Set;
import java.util.stream.Collectors;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseDatabasesApiHandler implements DatabasesApiHandler {
  private final DatabasesService databasesService;
  private final Validator validator;

  public OpenHouseDatabasesApiHandler(DatabasesService databasesService, Validator validator) {
    this.databasesService = databasesService;
    this.validator = validator;
  }

  @Override
  public ApiResponse<DatabaseMetadata> createDatabase(CreateDatabaseRequestBody request)
      throws DatabaseMetadataException {
    validateRequest(request);
    com.linkedin.openhouse.housetables.model.DatabaseMetadata metadata =
        databasesService.createDatabase(request.getDatabaseName(), request.getProperties());
    return ApiResponse.<DatabaseMetadata>builder()
        .httpStatus(HttpStatus.CREATED)
        .responseBody(
            DatabaseMetadata.builder()
                .databaseUuid(metadata.getDatabaseUuid())
                .databaseName(metadata.getDatabaseName())
                .properties(metadata.getProperties())
                .build())
        .build();
  }

  @Override
  public ApiResponse<DatabaseMetadata> getDatabase(String databaseName)
      throws DatabaseMetadataException {
    validateDatabaseName(databaseName);
    com.linkedin.openhouse.housetables.model.DatabaseMetadata metadata =
        databasesService.getDatabase(databaseName);
    return ApiResponse.<DatabaseMetadata>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            DatabaseMetadata.builder()
                .databaseUuid(metadata.getDatabaseUuid())
                .databaseName(metadata.getDatabaseName())
                .properties(metadata.getProperties())
                .build())
        .build();
  }

  @Override
  public ApiResponse<Void> deleteDatabase(String databaseName) throws DatabaseMetadataException {
    validateDatabaseName(databaseName);
    databasesService.deleteDatabase(databaseName);
    return ApiResponse.<Void>builder().httpStatus(HttpStatus.NO_CONTENT).build();
  }

  @Override
  public ApiResponse<UpdateDatabasePropertiesResponseBody> updateDatabaseProperties(
      String databaseName, UpdateDatabasePropertiesRequestBody request)
      throws DatabaseMetadataException {
    validateDatabaseName(databaseName);
    validateRequest(request);
    DatabasePropertyUpdate result =
        databasesService.updateDatabaseProperties(
            databaseName, request.getUpdates(), request.getRemovals());
    return ApiResponse.<UpdateDatabasePropertiesResponseBody>builder()
        .httpStatus(HttpStatus.OK)
        .responseBody(
            UpdateDatabasePropertiesResponseBody.builder()
                .updated(result.getUpdated())
                .removed(result.getRemoved())
                .missing(result.getMissing())
                .build())
        .build();
  }

  private void validateDatabaseName(String databaseName) throws DatabaseMetadataException {
    if (databaseName.isEmpty()
        || databaseName.length() > 128
        || !databaseName.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      throw new DatabaseMetadataException(
          INVALID_REQUEST, "Database name must contain 1 to 128 letters, digits, or underscores.");
    }
  }

  private <T> void validateRequest(T request) throws DatabaseMetadataException {
    Set<ConstraintViolation<T>> violations = validator.validate(request);
    if (!violations.isEmpty()) {
      throw new DatabaseMetadataException(
          INVALID_REQUEST,
          violations.stream()
              .map(violation -> violation.getPropertyPath() + ": " + violation.getMessage())
              .sorted()
              .collect(Collectors.joining("; ")));
    }
  }
}

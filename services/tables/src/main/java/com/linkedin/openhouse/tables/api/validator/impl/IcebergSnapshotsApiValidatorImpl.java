package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.INITIAL_TABLE_VERSION;

import com.linkedin.openhouse.common.api.validator.ApiValidatorUtil;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.api.validator.IcebergSnapshotsApiValidator;
import com.linkedin.openhouse.tables.api.validator.TablesApiValidator;
import java.util.ArrayList;
import java.util.List;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IcebergSnapshotsApiValidatorImpl implements IcebergSnapshotsApiValidator {
  @Autowired private TablesApiValidator tablesApiValidator;
  @Autowired private Validator validator;

  // Suppression needed as checkstyle and spotless doesn't agree to each other in terms of whether
  // `:` should be
  // in the same line with the for-loop main body or not.
  @SuppressWarnings("checkstyle:OperatorWrap")
  @Override
  public void validatePutSnapshots(
      String clusterId,
      String databaseId,
      String tableId,
      IcebergSnapshotsRequestBody icebergSnapshotsRequestBody) {
    List<String> validationFailures = new ArrayList<>();
    for (ConstraintViolation<IcebergSnapshotsRequestBody> violation :
        validator.validate(icebergSnapshotsRequestBody)) {
      validationFailures.add(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }

    // Only iff all constraints are fulfilled will it be safe to proceed to rest of check
    if (validationFailures.isEmpty()) {
      validateCreateUpdateTableRequestBody(
          clusterId,
          databaseId,
          tableId,
          icebergSnapshotsRequestBody.getBaseTableVersion(),
          icebergSnapshotsRequestBody.getCreateUpdateTableRequestBody(),
          validationFailures);
    }

    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  private void validateCreateUpdateTableRequestBody(
      String clusterId,
      String databaseId,
      String tableId,
      String baseTableVersion,
      CreateUpdateTableRequestBody requestBody,
      List<String> validationFailures) {
    if (requestBody == null) {
      validationFailures.add("Empty metadata body in Snapshot API is not allowed");
      return;
    }

    if (requestBody.isStageCreate()) {
      validationFailures.add(
          String.format(
              "Creating staged table %s.%s with putSnapshot is not supported",
              databaseId, tableId));
    }

    if (!baseTableVersion.equals(requestBody.getBaseTableVersion())) {
      validationFailures.add(
          "Base version provided in metadata and data request body is inconsistent. "
              + "Please consider use OpenHouse supported library to avoid such.");
    }

    if (baseTableVersion.equals(INITIAL_TABLE_VERSION)) {
      tablesApiValidator.validateCreateTable(clusterId, databaseId, requestBody);
    } else {
      tablesApiValidator.validateUpdateTable(clusterId, databaseId, tableId, requestBody);
    }
  }
}

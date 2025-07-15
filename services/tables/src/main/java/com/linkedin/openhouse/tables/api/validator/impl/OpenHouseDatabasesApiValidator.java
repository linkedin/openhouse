package com.linkedin.openhouse.tables.api.validator.impl;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_ERROR_MSG;
import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.ALPHA_NUM_UNDERSCORE_REGEX;

import com.linkedin.openhouse.common.api.validator.ApiValidatorUtil;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.validator.DatabasesApiValidator;
import java.util.ArrayList;
import java.util.List;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseDatabasesApiValidator implements DatabasesApiValidator {

  @Autowired private Validator validator;

  @SuppressWarnings("checkstyle:OperatorWrap")
  @Override
  public void validateUpdateAclPolicies(
      String databaseId, UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody) {
    List<String> validationFailures = new ArrayList<>();
    for (ConstraintViolation<UpdateAclPoliciesRequestBody> violation :
        validator.validate(updateAclPoliciesRequestBody)) {
      validationFailures.add(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }

    // Validate databaseId
    validateDatabaseId(databaseId);
  }

  @Override
  public void validateGetAclPolicies(String databaseId) {
    // Validate databaseId
    validateDatabaseId(databaseId);
  }

  @Override
  public void validateGetAllDatabases(int page, int size, String sortBy) {
    List<String> validationFailures = new ArrayList<>();
    if (page < 0) {
      validationFailures.add(String.format("page : provided %s, cannot be negative", page));
    }
    if (size <= 0) {
      validationFailures.add(String.format("size : provided %s, must be greater than 0", size));
    }
    if (sortBy != null && (sortBy.contains(",") || sortBy.contains(":"))) {
      validationFailures.add(
          String.format(
              "sortBy : provided %s, does not support multiple sort fields or directions", sortBy));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  private void validateDatabaseId(String databaseId) {
    List<String> validationFailures = new ArrayList<>();
    if (StringUtils.isEmpty(databaseId)) {
      validationFailures.add("databaseId : Cannot be empty");
    } else if (!databaseId.matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      validationFailures.add(
          String.format("databaseId provided: %s, %s", databaseId, ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }
}

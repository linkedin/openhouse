package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.validator.ApiValidatorUtil;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.UpdateAclPoliciesRequestBody;
import com.linkedin.openhouse.tables.api.validator.DatabasesApiValidator;
import java.util.ArrayList;
import java.util.List;
import javax.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OpenHouseDatabasesApiValidator implements DatabasesApiValidator {

  @Autowired private Validator validator;

  @Override
  public void validateUpdateAclPolicies(
      String databaseId, UpdateAclPoliciesRequestBody updateAclPoliciesRequestBody) {
    List<String> validationFailures = new ArrayList<>();
    ApiValidatorUtil.collectViolations(validator, updateAclPoliciesRequestBody, validationFailures);
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
    ApiValidatorUtil.validatePageable(page, size, sortBy, validationFailures);
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  private void validateDatabaseId(String databaseId) {
    List<String> validationFailures = new ArrayList<>();
    ApiValidatorUtil.validateIdentifier("databaseId", databaseId, validationFailures);
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }
}

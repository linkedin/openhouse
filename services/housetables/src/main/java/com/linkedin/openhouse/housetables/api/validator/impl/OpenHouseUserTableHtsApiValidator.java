package com.linkedin.openhouse.housetables.api.validator.impl;

import static com.linkedin.openhouse.common.api.validator.ValidatorConstants.*;

import com.linkedin.openhouse.common.api.validator.ApiValidatorUtil;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.api.spec.model.UserTableKey;
import com.linkedin.openhouse.housetables.api.validator.HouseTablesApiValidator;
import java.util.ArrayList;
import java.util.List;
import javax.validation.ConstraintViolation;
import javax.validation.Validator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/** Class implementing validations for all /hts/tables REST endpoints. */
@Component
public class OpenHouseUserTableHtsApiValidator
    implements HouseTablesApiValidator<UserTableKey, UserTable> {

  @Autowired private Validator validator;

  @Override
  public void validateGetEntity(UserTableKey userTableKey) {
    List<String> validationFailures = new ArrayList<>();
    if (!userTableKey.getDatabaseId().matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      validationFailures.add(
          String.format(
              "databaseId provided: %s, %s",
              userTableKey.getDatabaseId(), ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    }
    if (!userTableKey.getTableId().matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      validationFailures.add(
          String.format(
              "tableId provided: %s, %s",
              userTableKey.getTableId(), ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    }
    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  @Override
  public void validateDeleteEntity(UserTableKey userTableKey) {
    // Validation is similar to GetEntity.
    validateGetEntity(userTableKey);
  }

  @Override
  public void validateGetEntities(UserTable userTable) {
    List<String> validationFailures = new ArrayList<>();

    if (userTable.getDatabaseId() != null
        && !userTable.getDatabaseId().matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      validationFailures.add(
          String.format(
              "databaseId provided: %s, %s",
              userTable.getDatabaseId(), ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    }

    if (userTable.getTableId() != null
        && !userTable.getTableId().matches(ALPHA_NUM_UNDERSCORE_REGEX)) {
      validationFailures.add(
          String.format(
              "tableId provided: %s, %s", userTable.getTableId(), ALPHA_NUM_UNDERSCORE_ERROR_MSG));
    }

    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }

  @Override
  public void validatePutEntity(UserTable userTable) {
    List<String> validationFailures = new ArrayList<>();

    for (ConstraintViolation<UserTable> violation : validator.validate(userTable)) {
      validationFailures.add(
          String.format("%s : %s", ApiValidatorUtil.getField(violation), violation.getMessage()));
    }

    if (!validationFailures.isEmpty()) {
      throw new RequestValidationFailureException(validationFailures);
    }
  }
}

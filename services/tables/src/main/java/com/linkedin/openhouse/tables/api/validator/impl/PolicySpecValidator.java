package com.linkedin.openhouse.tables.api.validator.impl;

import com.linkedin.openhouse.common.api.spec.TableUri;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;

public abstract class PolicySpecValidator {

  protected String failureMessage = "";

  protected String errorField = "";

  public abstract boolean validate(
      CreateUpdateTableRequestBody createUpdateTableRequestBody, TableUri tableUri);

  public String getField() {
    return errorField;
  }

  public String getMessage() {
    return failureMessage;
  }
}

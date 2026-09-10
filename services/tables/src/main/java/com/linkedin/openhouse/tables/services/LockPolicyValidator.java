package com.linkedin.openhouse.tables.services;

import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.components.LockState;
import com.linkedin.openhouse.tables.model.TableDto;

/** Prevent table and snapshot writes from bypassing the guarded lock API. */
final class LockPolicyValidator {
  private LockPolicyValidator() {}

  static void validateUnchanged(TableDto tableDto, CreateUpdateTableRequestBody requestBody) {
    LockState requested =
        requestBody.getPolicies() == null ? null : requestBody.getPolicies().getLockState();
    LockState existing =
        tableDto == null || tableDto.getPolicies() == null
            ? null
            : tableDto.getPolicies().getLockState();
    if (requested != null
        && (requested.getReason() != null || (existing != null && existing.getReason() != null))
        && !requested.equals(existing)) {
      throw new RequestValidationFailureException(
          "Reasoned lock state can only be changed through the lock API");
    }
  }
}

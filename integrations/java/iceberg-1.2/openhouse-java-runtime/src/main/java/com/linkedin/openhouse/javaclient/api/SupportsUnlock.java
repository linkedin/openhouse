package com.linkedin.openhouse.javaclient.api;

import com.linkedin.openhouse.tables.client.model.CreateUpdateLockRequestBody.ReasonEnum;

public interface SupportsUnlock {

  /**
   * Check LOCK_ADMIN for the authenticated caller without loading or mutation. Fail closed if the
   * check cannot be completed.
   */
  void checkUnlockAuthorization(UnlockTableTarget target);

  /**
   * Unlock without loading the table; recheck authorization before mutation.
   *
   * @param target validated database/table target
   * @param reason non-null lock reason; SQL defaults an omitted reason to LEGACY
   */
  void unlockTable(UnlockTableTarget target, ReasonEnum reason);
}

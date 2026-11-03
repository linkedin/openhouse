package com.linkedin.openhouse.javaclient.api;

import com.linkedin.openhouse.tables.client.model.CreateUpdateLockRequestBody.ReasonEnum;

public interface SupportsUnlock {

  /**
   * Unlock without loading the table; enforce authorization before mutation.
   *
   * @param target validated database/table target
   * @param reason non-null lock reason; SQL defaults an omitted reason to LEGACY
   */
  void unlockTable(UnlockTableTarget target, ReasonEnum reason);
}

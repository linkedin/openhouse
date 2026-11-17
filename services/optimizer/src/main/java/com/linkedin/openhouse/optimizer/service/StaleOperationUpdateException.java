package com.linkedin.openhouse.optimizer.service;

/**
 * Thrown when a completion callback no longer owns its operation attempt: the attempt was already
 * resolved (a duplicate callback, or the analyzer expired it after the stale-timeout) or was never
 * claimed. The callback is rejected and nothing is written, so a stale outcome can never overwrite
 * or supersede the attempt's recorded result.
 */
public class StaleOperationUpdateException extends RuntimeException {

  public StaleOperationUpdateException(String operationId) {
    super(
        String.format(
            "operation %s is not awaiting a result (already resolved, expired, or never claimed)",
            operationId));
  }
}

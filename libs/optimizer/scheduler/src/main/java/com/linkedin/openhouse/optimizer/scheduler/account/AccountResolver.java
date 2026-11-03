package com.linkedin.openhouse.optimizer.scheduler.account;

/** Resolves a table creator to the account used for job execution. */
@FunctionalInterface
public interface AccountResolver {
  String resolve(String creator);
}

package com.linkedin.openhouse.optimizer.scheduler.account;

/** A dummy account resolver that uses the table creator as the account. */
public class DummyAccountResolver implements AccountResolver {
  @Override
  public String resolve(String creator) {
    return creator;
  }
}

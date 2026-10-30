package com.linkedin.openhouse.javaclient.precommit;

/**
 * Lets a caller attach a {@link PreCommitHook} to something that commits.
 *
 * <p>Registration is a capability you compose onto an object, rather than something built into the
 * commit path. The commit code stays as it is; it just calls whatever hook is registered.
 */
public interface SupportsPreCommit {

  /**
   * Registers the check to run before each commit. Replaces any hook already registered.
   *
   * @return this, so it can be chained onto a newly built instance
   */
  SupportsPreCommit withPreCommit(PreCommitHook hook);
}

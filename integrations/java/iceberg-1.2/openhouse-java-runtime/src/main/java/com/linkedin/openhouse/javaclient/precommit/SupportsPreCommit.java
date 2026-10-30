package com.linkedin.openhouse.javaclient.precommit;

/**
 * Lets a caller attach a {@link PreCommitHook} to something that commits, or to something that
 * creates them.
 *
 * <p>Registration is a capability you compose onto an object, rather than something built into the
 * commit path. The commit code stays as it is; it just calls whatever hook is registered.
 */
public interface SupportsPreCommit {

  /**
   * Adds a check to run before each commit. Hooks run in registration order and the first to block
   * stops the commit, so registering here cannot switch off a hook registered earlier.
   *
   * @return this, so it can be chained onto a newly built instance
   */
  SupportsPreCommit withPreCommit(PreCommitHook hook);
}

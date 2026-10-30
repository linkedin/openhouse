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
   * Adds a check to run before each commit. Hooks run in the order they were registered, and the
   * first one to block stops the commit -- so a hook registered here cannot switch off one
   * registered earlier.
   *
   * @return this, so it can be chained onto a newly built instance
   */
  SupportsPreCommit withPreCommit(PreCommitHook hook);
}

package com.linkedin.openhouse.javaclient.precommit;

/**
 * A check that runs just before a commit is sent to the OpenHouse service.
 *
 * <p>Return normally to let the commit through. Throw {@link PreCommitHookBlockedException} to stop
 * it. Nothing has been sent when the hook runs, so a thrown hook leaves the table untouched.
 *
 * <p>Hooks are expected to be quick and side-effect free: this runs inline on the commit path, and
 * Iceberg may retry a commit, which calls the hook again.
 *
 * <p>One hook instance serves every commit on the table operations it was registered with, so
 * implementations must be thread safe.
 */
@FunctionalInterface
public interface PreCommitHook {

  /** Allows every commit. The default, and what you get when no hook is registered. */
  PreCommitHook NOOP = context -> {};

  /**
   * Checks a commit that is about to be sent.
   *
   * @param context what the commit is doing
   * @throws PreCommitHookBlockedException to stop the commit
   */
  void evaluate(PreCommitHookContext context) throws PreCommitHookBlockedException;
}

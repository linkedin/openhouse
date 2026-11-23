package com.linkedin.openhouse.javaclient.precommit;

/**
 * A check that runs just before a commit is sent to the OpenHouse service.
 *
 * <p>A hook runs only for tables carrying {@code openhouse.preCommitHooksEnabled=true}. Registering
 * one is the first half, the table opting in is the second, and the flag is read from the table as
 * it stands so a commit cannot clear it to escape the hooks judging that same commit.
 *
 * <p>Three things a hook must honour:
 *
 * <ul>
 *   <li><b>Check only.</b> Return to allow, throw {@link PreCommitHookRejectedException} to reject.
 *       Never change the commit. Nothing has been sent when the hook runs, so a thrown hook leaves
 *       the table untouched.
 *   <li><b>Synchronous and bounded.</b> This runs inline on the committing thread. A hook that
 *       calls another service owns its own timeout, and reports that service being unreachable as
 *       {@code OPERATIONAL} rather than as a rejection.
 *   <li><b>Current base only.</b> This runs after {@code doCommit} has settled what the commit
 *       does, so a hook only ever judges a commit built on the table's current version. Iceberg may
 *       retry a commit, which calls the hook again with the new base.
 * </ul>
 *
 * <p>One hook instance serves every commit on the table operations it was registered with, so
 * implementations must be thread safe.
 */
@FunctionalInterface
public interface PreCommitHook {

  /** Allows every commit. Same effect as registering nothing. */
  PreCommitHook NOOP = context -> {};

  /**
   * Checks a commit that is about to be sent.
   *
   * @param context what the commit is doing
   * @throws PreCommitHookRejectedException to stop the commit
   */
  void evaluate(PreCommitHookContext context) throws PreCommitHookRejectedException;

  /**
   * A short, stable name for this hook, used in logs and error messages.
   *
   * <p>Keep it low cardinality: a constant like {@code "row-count-check"}, never a value built from
   * the table or the commit, so it can safely be used as a metric tag.
   */
  default String getName() {
    return getClass().getSimpleName();
  }
}

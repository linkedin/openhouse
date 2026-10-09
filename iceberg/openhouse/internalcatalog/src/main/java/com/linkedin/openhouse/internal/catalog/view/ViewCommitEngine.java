package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;

/**
 * Publishes view metadata with a native HTS commit strategy. Commit is the only operation: CREATE
 * and REPLACE write an immutable metadata file and attempt one typed HTS compare-and-swap.
 */
public interface ViewCommitEngine {

  /**
   * Commits against the supplied snapshot without reloading HTS. CREATE requires absence; REPLACE
   * requires a view. No-op REPLACE returns the captured snapshot without publishing. The source
   * dialect is required on CREATE and immutable on REPLACE: it must exactly (case-sensitively)
   * match the current version's source dialect in the captured snapshot.
   */
  ViewCommitResult commit(ViewCommitIntent intent);
}

package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * The view analogue of {@code OpenHouseInternalCatalog} plus {@code
 * OpenHouseInternalTableOperations}. It never generates identity, selects storage, or allocates a
 * root; those arrive on {@link ViewCommitIntent}.
 *
 * <p>Every signature is version-neutral, so the interface stays loadable under Iceberg 1.2.
 */
public interface ViewCommitEngine {

  /**
   * Commits one view under the caller's explicit {@link ViewCommitIntent#getOperation()}, against
   * the trusted snapshot in {@link ViewCommitIntent#getBaseRow()} (a hydrated row, or {@code null}
   * for a completed lookup that found absence). It classifies and swaps against that snapshot and
   * performs no House Table read of its own. A CREATE against a taken name and a REPLACE of an
   * absent or non-view target are rejected; an identical-definition REPLACE is a snapshot no-op
   * that publishes nothing and may return the captured (possibly stale) pointer. A missing
   * operation fails before any effect.
   */
  ViewCommitResult commit(ViewCommitIntent intent);

  /** Reads a view; a key that is absent or holds a non-view throws {@code NoSuchViewException}. */
  LoadedView loadView(String databaseId, String viewId);

  /** Lists view pointers for a database without reading any metadata file. */
  Page<ViewPointer> listViews(String databaseId, Pageable pageable);

  /** Hard delete, no storage cleanup; false when the key is absent or holds a non-view. */
  boolean dropView(String databaseId, String viewId);

  /** Always throws {@code UnsupportedOperationException}, before touching any repository. */
  void renameView(String databaseId, String fromViewId, String toViewId);
}

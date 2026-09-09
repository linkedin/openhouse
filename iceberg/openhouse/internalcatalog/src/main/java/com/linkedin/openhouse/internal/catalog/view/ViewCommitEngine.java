package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * View counterpart to {@code OpenHouseInternalCatalog} and {@code
 * OpenHouseInternalTableOperations}, with signatures usable under Iceberg 1.2.
 */
public interface ViewCommitEngine {

  /**
   * Commits against the supplied snapshot without reloading HTS. CREATE requires absence; REPLACE
   * requires a view. No-op REPLACE returns the captured snapshot without publishing.
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

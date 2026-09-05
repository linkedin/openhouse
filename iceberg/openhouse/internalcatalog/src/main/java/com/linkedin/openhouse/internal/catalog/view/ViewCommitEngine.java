package com.linkedin.openhouse.internal.catalog.view;

import com.linkedin.openhouse.internal.catalog.view.model.LoadedView;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitIntent;
import com.linkedin.openhouse.internal.catalog.view.model.ViewCommitResult;
import com.linkedin.openhouse.internal.catalog.view.model.ViewPointer;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * The view analogue of {@code OpenHouseInternalCatalog} plus {@code
 * OpenHouseInternalTableOperations}: typed pointer read, Iceberg metadata build, immutable-file
 * write, and exactly one House Table compare-and-swap. It is catalog-equivalent down to the shared
 * House Table API, not a service repository, so it never generates entity identity, selects
 * storage, or allocates a root; those arrive on {@link ViewCommitIntent}.
 *
 * <p>Every signature is version-neutral so the interface stays loadable under Iceberg 1.2, where no
 * implementation bean is registered.
 */
public interface ViewCommitEngine {

  /**
   * A null {@link ViewCommitIntent#getBaseViewVersion()} is a CREATE; a non-null value is a REPLACE
   * against that exact current metadata path. CREATE requires {@link
   * ViewCommitIntent#getViewUuid()}, {@link ViewCommitIntent#getViewLocation()}, and {@link
   * ViewCommitIntent#getStorageType()}; REPLACE ignores all three and preserves the published ones.
   *
   * @throws org.apache.iceberg.exceptions.AlreadyExistsException create collided with a view, or
   *     lost the create compare-and-swap
   * @throws ViewNameOccupiedException create collided with a non-view occupant
   * @throws org.apache.iceberg.exceptions.CommitFailedException replace token was stale, or lost
   *     the replace compare-and-swap
   * @throws org.apache.iceberg.exceptions.CommitStateUnknownException the single publish attempt
   *     was ambiguous; never retried
   * @throws org.apache.iceberg.exceptions.BadRequestException the caller supplied duplicate
   *     dialects, a server-owned property, or omitted a required create-side field
   * @throws org.apache.iceberg.exceptions.NoSuchViewException replace targeted a key that is absent
   *     or holds a non-view
   */
  ViewCommitResult commit(ViewCommitIntent intent);

  /**
   * @throws org.apache.iceberg.exceptions.NoSuchViewException the key is absent or holds a non-view
   */
  LoadedView loadView(String databaseId, String viewId);

  /** Lists view pointers for a database without reading any metadata file. */
  Page<ViewPointer> listViews(String databaseId, Pageable pageable);

  /**
   * Hard delete; no metadata parse and no storage deletion.
   *
   * @return false when the key is absent or holds a non-view
   * @throws org.apache.iceberg.exceptions.CommitStateUnknownException the single delete attempt was
   *     ambiguous; never retried
   */
  boolean dropView(String databaseId, String viewId);

  /** @throws UnsupportedOperationException always, before any repository or storage interaction */
  void renameView(String databaseId, String fromViewId, String toViewId);
}

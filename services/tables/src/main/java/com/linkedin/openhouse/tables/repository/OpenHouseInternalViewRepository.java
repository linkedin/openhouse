package com.linkedin.openhouse.tables.repository;

import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateViewRequestBody;
import com.linkedin.openhouse.tables.model.ViewDto;
import com.linkedin.openhouse.tables.repository.impl.PreparedViewOperation;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

/**
 * Service repository seam for views, mirroring {@code OpenHouseInternalRepository} for tables
 * without reusing its implementation: no snapshots, partition specs, sort orders, or retention
 * semantics. Owns the single pre-admission snapshot capture ({@link #prepareWrite}/{@link
 * #prepareDelete}), UUID/storage/root allocation (create only, after admission), the {@code
 * ViewCommitEngine} call, and result mapping. Surfaces engine and HTS exceptions unwrapped: typed,
 * cause-preserving translation happens once, at the {@code ViewsServiceImpl} boundary.
 */
public interface OpenHouseInternalViewRepository {

  /**
   * The single neutral POST/PUT capture (any entity type), taken once before authorization and
   * reused through base-version checking, admission, the engine commit, and audit.
   */
  PreparedViewOperation prepareWrite(String databaseId, String viewId);

  /** The single typed DELETE capture: a table at this key reads as absent, like a GET. */
  PreparedViewOperation prepareDelete(String databaseId, String viewId);

  /**
   * Pointer-only read, served from the HTS row with no metadata-file parse.
   *
   * @throws com.linkedin.openhouse.tables.exception.ViewApiException with {@link
   *     com.linkedin.openhouse.tables.exception.ViewErrorCode#NO_SUCH_VIEW} if the key is absent or
   *     holds a non-view.
   */
  ViewDto findById(String databaseId, String viewId);

  /** Pointer-only listing, served from HTS rows with no metadata-file parse. */
  Page<ViewDto> searchViews(String databaseId, Pageable pageable);

  /** Allocates UUID/storage/root, then commits a CREATE using the captured (absent) snapshot. */
  ViewCommitOutcome commitCreate(
      CreateUpdateViewRequestBody requestBody,
      PreparedViewOperation prepared,
      String actingPrincipal);

  /** Commits a REPLACE using the exact captured row as the CAS token; no refresh. */
  ViewCommitOutcome commitReplace(
      CreateUpdateViewRequestBody requestBody,
      PreparedViewOperation prepared,
      String actingPrincipal);

  /** Name-based hard delete, matching table DELETE semantics. */
  void deleteById(String databaseId, String viewId);
}

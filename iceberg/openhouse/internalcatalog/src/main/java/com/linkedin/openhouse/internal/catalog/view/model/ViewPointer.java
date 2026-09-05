package com.linkedin.openhouse.internal.catalog.view.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The House Table resident pointer row for a view: identifiers, the current metadata path that
 * doubles as the public version token, the storage the row lives on, and creation time.
 *
 * <p>A pointer never carries UUID: House Table has no dedicated UUID column for it, so supplying
 * one would require parsing every metadata file. UUID appears on {@link ViewCommitResult} and
 * {@link LoadedView} only.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class ViewPointer {

  private final String databaseId;

  private final String viewId;

  /** Current metadata path; also the public compare-and-swap token. */
  private final String metadataLocation;

  private final String storageType;

  private final long creationTime;
}

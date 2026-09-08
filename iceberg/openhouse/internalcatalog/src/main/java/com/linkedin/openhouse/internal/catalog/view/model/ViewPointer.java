package com.linkedin.openhouse.internal.catalog.view.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * The House Table resident pointer row for a view.
 *
 * <p>No UUID: House Table has no column for it, so supplying one would mean parsing every metadata
 * file. It appears on {@link ViewCommitResult} and {@link LoadedView} instead.
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

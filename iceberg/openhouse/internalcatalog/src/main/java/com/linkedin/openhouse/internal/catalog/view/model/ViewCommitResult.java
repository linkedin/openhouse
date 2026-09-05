package com.linkedin.openhouse.internal.catalog.view.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/**
 * Outcome of one view commit.
 *
 * <p>{@code metadataChanged} lets the later service layer distinguish a real commit from an
 * identical-definition replace, which writes no metadata file and performs no compare-and-swap.
 */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class ViewCommitResult {

  private final ViewPointer pointer;

  /** Iceberg {@code view-uuid}, which is always equal to {@code openhouse.tableUUID}. */
  private final String viewUuid;

  private final long lastModifiedTime;

  private final boolean created;

  /** False for an identical-definition replace: no file written and no pointer movement. */
  private final boolean metadataChanged;
}

package com.linkedin.openhouse.internal.catalog.view.model;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

/** Outcome of one view commit. */
@Builder(toBuilder = true)
@Getter
@EqualsAndHashCode
@ToString
public class ViewCommitResult {

  private final ViewPointer pointer;

  private final String viewUuid;

  private final long lastModifiedTime;

  private final boolean created;

  /** False for a snapshot no-op; concurrent HTS state may still change. */
  private final boolean metadataChanged;
}

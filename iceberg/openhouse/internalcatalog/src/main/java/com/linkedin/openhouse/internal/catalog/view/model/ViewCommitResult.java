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

  /** False for an identical-definition replace: no file written and no pointer movement. */
  private final boolean metadataChanged;
}

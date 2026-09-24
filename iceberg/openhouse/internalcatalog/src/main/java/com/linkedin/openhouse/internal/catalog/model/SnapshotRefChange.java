package com.linkedin.openhouse.internal.catalog.model;

import lombok.Builder;
import lombok.Value;

/** A successfully applied ref transition at an index in the ordered metadata update list. */
@Value
@Builder
public class SnapshotRefChange {
  int updateIndex;
  String action;
  String refName;
  RefState before;
  RefState after;

  /** Ref state including retention settings; a null state denotes an absent ref. */
  @Value
  @Builder
  public static class RefState {
    long snapshotId;
    String type;
    Integer minSnapshotsToKeep;
    Long maxSnapshotAgeMs;
    Long maxRefAgeMs;
  }
}

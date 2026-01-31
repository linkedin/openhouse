package com.linkedin.openhouse.tables.model;

import lombok.Builder;
import lombok.Value;

/**
 * Model representing a diff between two consecutive Iceberg metadata versions. Contains both the
 * current and previous metadata.json content for client-side diffing.
 */
@Builder(toBuilder = true)
@Value
public class IcebergMetadataDiff {
  String tableId;
  String databaseId;

  // Current commit metadata
  String currentMetadata;
  String currentSnapshotId; // String to preserve precision in JavaScript
  Long currentTimestamp;
  String currentMetadataLocation;

  // Previous commit metadata (null if this is the first commit)
  String previousMetadata;
  String previousSnapshotId; // String to preserve precision in JavaScript
  Long previousTimestamp;
  String previousMetadataLocation;

  // Indicates if this is the first commit (no previous metadata)
  Boolean isFirstCommit;
}

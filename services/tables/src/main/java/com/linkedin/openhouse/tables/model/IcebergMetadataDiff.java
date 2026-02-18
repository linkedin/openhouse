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

  // Current metadata
  String currentMetadata;
  Long currentTimestamp;
  String currentMetadataLocation;

  // Previous metadata (null if this is the first entry in metadata-log)
  String previousMetadata;
  Long previousTimestamp;
  String previousMetadataLocation;

  // Indicates if this is the first entry in metadata-log (no previous metadata)
  Boolean isFirstCommit;
}

package com.linkedin.openhouse.tables.model;

import java.util.List;
import lombok.Builder;
import lombok.Value;

/** Internal model for table metadata containing rich Iceberg metadata */
@Builder(toBuilder = true)
@Value
public class IcebergMetadata {
  String tableId;
  String databaseId;
  String currentMetadata;
  List<MetadataVersion> metadataHistory;
  String metadataLocation;
  String snapshots;
  String partitions;
  String currentSnapshotId; // String to preserve precision in JavaScript

  /** Represents a metadata version entry */
  @Builder
  @Value
  public static class MetadataVersion {
    Integer version;
    String file;
    Long timestamp;
    String location;
  }
}

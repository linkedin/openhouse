package com.linkedin.openhouse.common.stats.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.experimental.SuperBuilder;

/** Data Model for capturing iceberg stats about a table. */
@Getter
@NoArgsConstructor
@AllArgsConstructor
@SuperBuilder(toBuilder = true)
public class IcebergTableStats extends BaseTableMetadata {

  private Long totalReferencedDataFilesSizeInBytes;

  private Long numReferencedDataFiles;

  private Long totalDirectorySizeInBytes;

  private Long numObjectsInDirectory;

  private Long currentSnapshotId;

  private Long currentSnapshotTimestamp;

  private Long numCurrentSnapshotReferencedDataFiles;

  private Long totalCurrentSnapshotReferencedDataFilesSizeInBytes;

  private Long oldestSnapshotTimestamp;

  private Long numExistingMetadataJsonFiles;

  private Long numReferencedManifestFiles;

  private Long numReferencedManifestLists;
}

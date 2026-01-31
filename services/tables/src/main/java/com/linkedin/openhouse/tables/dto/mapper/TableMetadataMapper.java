package com.linkedin.openhouse.tables.dto.mapper;

import com.linkedin.openhouse.tables.api.spec.v0.response.GetIcebergMetadataResponseBody;
import com.linkedin.openhouse.tables.api.spec.v0.response.GetMetadataDiffResponseBody;
import com.linkedin.openhouse.tables.model.IcebergMetadata;
import com.linkedin.openhouse.tables.model.IcebergMetadataDiff;
import java.util.stream.Collectors;
import org.springframework.stereotype.Component;

/** Mapper for converting TableMetadata model to response body */
@Component
public class TableMetadataMapper {

  /**
   * Convert TableMetadata model to GetTableMetadataResponseBody
   *
   * @param metadata TableMetadata model
   * @return GetTableMetadataResponseBody
   */
  public GetIcebergMetadataResponseBody toResponseBody(IcebergMetadata metadata) {
    if (metadata == null) {
      return null;
    }

    return GetIcebergMetadataResponseBody.builder()
        .tableId(metadata.getTableId())
        .databaseId(metadata.getDatabaseId())
        .currentMetadata(metadata.getCurrentMetadata())
        .metadataHistory(
            metadata.getMetadataHistory() != null
                ? metadata.getMetadataHistory().stream()
                    .map(this::toMetadataVersionResponse)
                    .collect(Collectors.toList())
                : null)
        .metadataLocation(metadata.getMetadataLocation())
        .snapshots(metadata.getSnapshots())
        .partitions(metadata.getPartitions())
        .currentSnapshotId(metadata.getCurrentSnapshotId())
        .build();
  }

  /**
   * Convert IcebergMetadataDiff model to GetMetadataDiffResponseBody
   *
   * @param diff IcebergMetadataDiff model
   * @return GetMetadataDiffResponseBody
   */
  public GetMetadataDiffResponseBody toMetadataDiffResponseBody(IcebergMetadataDiff diff) {
    if (diff == null) {
      return null;
    }

    return GetMetadataDiffResponseBody.builder()
        .tableId(diff.getTableId())
        .databaseId(diff.getDatabaseId())
        .currentMetadata(diff.getCurrentMetadata())
        .currentSnapshotId(diff.getCurrentSnapshotId())
        .currentTimestamp(diff.getCurrentTimestamp())
        .currentMetadataLocation(diff.getCurrentMetadataLocation())
        .previousMetadata(diff.getPreviousMetadata())
        .previousSnapshotId(diff.getPreviousSnapshotId())
        .previousTimestamp(diff.getPreviousTimestamp())
        .previousMetadataLocation(diff.getPreviousMetadataLocation())
        .isFirstCommit(diff.getIsFirstCommit())
        .build();
  }

  /**
   * Convert TableMetadata.MetadataVersion to GetTableMetadataResponseBody.MetadataVersion
   *
   * @param version TableMetadata.MetadataVersion
   * @return GetTableMetadataResponseBody.MetadataVersion
   */
  private GetIcebergMetadataResponseBody.MetadataVersion toMetadataVersionResponse(
      IcebergMetadata.MetadataVersion version) {
    if (version == null) {
      return null;
    }

    return GetIcebergMetadataResponseBody.MetadataVersion.builder()
        .version(version.getVersion())
        .file(version.getFile())
        .timestamp(version.getTimestamp())
        .location(version.getLocation())
        .build();
  }
}

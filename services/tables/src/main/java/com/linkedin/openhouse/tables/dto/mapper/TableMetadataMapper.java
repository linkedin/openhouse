package com.linkedin.openhouse.tables.dto.mapper;

import com.linkedin.openhouse.tables.api.spec.v0.response.GetIcebergMetadataResponseBody;
import com.linkedin.openhouse.tables.model.IcebergMetadata;
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

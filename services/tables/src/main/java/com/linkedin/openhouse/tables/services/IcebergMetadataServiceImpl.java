package com.linkedin.openhouse.tables.services;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.common.exception.NoSuchUserTableException;
import com.linkedin.openhouse.tables.authorization.Privileges;
import com.linkedin.openhouse.tables.model.IcebergMetadata;
import com.linkedin.openhouse.tables.model.TableDto;
import com.linkedin.openhouse.tables.model.TableDtoPrimaryKey;
import com.linkedin.openhouse.tables.repository.OpenHouseInternalRepository;
import com.linkedin.openhouse.tables.utils.AuthorizationUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class IcebergMetadataServiceImpl implements IcebergMetadataService {

  @Autowired Catalog catalog;

  @Autowired AuthorizationUtils authorizationUtils;

  @Autowired OpenHouseInternalRepository openHouseInternalRepository;

  private static final Gson GSON = new Gson();

  @Override
  public IcebergMetadata getIcebergMetadata(
      String databaseId, String tableId, String actingPrincipal) {
    TableDto tableDto = getTableOrThrow(databaseId, tableId);
    authorizationUtils.checkTablePrivilege(
        tableDto, actingPrincipal, Privileges.GET_TABLE_METADATA);

    // Load the Iceberg table from catalog to access full metadata
    Table icebergTable = catalog.loadTable(TableIdentifier.of(databaseId, tableId));

    // Extract metadata.json once
    String currentMetadataJson = extractCurrentMetadata(icebergTable);
    JsonObject metadataObj = null;
    if (currentMetadataJson != null) {
      metadataObj = GSON.fromJson(currentMetadataJson, JsonObject.class);
    }

    // Extract snapshots from metadata.json
    String snapshotsJson = extractSnapshotsFromMetadata(metadataObj);

    // Extract current snapshot ID from metadata.json
    Long currentSnapshotId = extractCurrentSnapshotIdFromMetadata(metadataObj);

    // Extract metadata history
    List<IcebergMetadata.MetadataVersion> metadataHistory = extractMetadataHistory(icebergTable);

    // Extract partition specs from metadata (no file scanning)
    String partitionsJson = extractPartitionsJson(icebergTable);

    // Build and return metadata model
    return IcebergMetadata.builder()
        .tableId(tableId)
        .databaseId(databaseId)
        .currentMetadata(currentMetadataJson)
        .metadataHistory(metadataHistory)
        .metadataLocation(icebergTable.location() + "/metadata")
        .snapshots(snapshotsJson)
        .partitions(partitionsJson)
        .currentSnapshotId(currentSnapshotId)
        .build();
  }

  /**
   * Extract snapshots from parsed metadata.json
   *
   * @param metadataObj Parsed metadata JSON object
   * @return JSON string of snapshots array, or null if not available
   */
  private String extractSnapshotsFromMetadata(JsonObject metadataObj) {
    if (metadataObj == null || !metadataObj.has("snapshots")) {
      return null;
    }

    try {
      JsonArray snapshotsArray = metadataObj.getAsJsonArray("snapshots");
      if (snapshotsArray == null || snapshotsArray.size() == 0) {
        return null;
      }

      // Transform snapshots to match expected format
      List<java.util.Map<String, Object>> snapshotsList = new ArrayList<>();
      for (JsonElement snapshotElement : snapshotsArray) {
        JsonObject snapshot = snapshotElement.getAsJsonObject();
        java.util.Map<String, Object> snapshotInfo = new java.util.HashMap<>();

        if (snapshot.has("snapshot-id")) {
          snapshotInfo.put("snapshot-id", snapshot.get("snapshot-id").getAsLong());
        }
        if (snapshot.has("timestamp-ms")) {
          snapshotInfo.put("timestamp-ms", snapshot.get("timestamp-ms").getAsLong());
        }
        if (snapshot.has("operation")) {
          snapshotInfo.put("operation", snapshot.get("operation").getAsString());
        }
        if (snapshot.has("summary")) {
          JsonObject summaryObj = snapshot.getAsJsonObject("summary");
          java.util.Map<String, String> summary = new java.util.HashMap<>();
          for (String key : summaryObj.keySet()) {
            summary.put(key, summaryObj.get(key).getAsString());
          }
          snapshotInfo.put("summary", summary);
        }
        if (snapshot.has("parent-snapshot-id")) {
          snapshotInfo.put("parent-snapshot-id", snapshot.get("parent-snapshot-id").getAsLong());
        }

        snapshotsList.add(snapshotInfo);
      }

      return GSON.toJson(snapshotsList);
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extract current snapshot ID from parsed metadata.json
   *
   * @param metadataObj Parsed metadata JSON object
   * @return Current snapshot ID, or null if not available
   */
  private Long extractCurrentSnapshotIdFromMetadata(JsonObject metadataObj) {
    if (metadataObj == null || !metadataObj.has("current-snapshot-id")) {
      return null;
    }

    try {
      return metadataObj.get("current-snapshot-id").getAsLong();
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extract current metadata.json content
   *
   * @param table Iceberg table
   * @return JSON string of current metadata, or null if not available
   */
  private String extractCurrentMetadata(Table table) {
    if (table == null) {
      return null;
    }

    try {
      // Get the table's FileIO
      org.apache.iceberg.io.FileIO io = table.io();

      // Get the current metadata file path from table operations
      String currentMetadataFile = null;
      try {
        org.apache.iceberg.TableOperations ops =
            ((org.apache.iceberg.BaseTable) table).operations();
        if (ops != null && ops.current() != null) {
          currentMetadataFile = ops.current().metadataFileLocation();
        }
      } catch (Exception e) {
        // Fallback: try to construct path from table location
      }

      // List of paths to try
      java.util.List<String> pathsToTry = new java.util.ArrayList<>();
      if (currentMetadataFile != null) {
        pathsToTry.add(currentMetadataFile);
      }

      // Try parsing the metadata file
      for (String path : pathsToTry) {
        try {
          org.apache.iceberg.io.InputFile inputFile = io.newInputFile(path);
          org.apache.iceberg.TableMetadata metadata =
              org.apache.iceberg.TableMetadataParser.read(io, inputFile);

          if (metadata != null) {
            // Convert metadata to JSON
            return org.apache.iceberg.TableMetadataParser.toJson(metadata);
          }
        } catch (Exception e) {
          // Try next path
        }
      }

      // Fallback: return basic info if we can't parse the metadata file
      java.util.Map<String, Object> metadataInfo = new java.util.HashMap<>();
      metadataInfo.put("location", table.location());
      metadataInfo.put("uuid", table.uuid());
      if (table.currentSnapshot() != null) {
        metadataInfo.put("current-snapshot-id", table.currentSnapshot().snapshotId());
      }
      metadataInfo.put("format-version", table.properties().getOrDefault("format-version", "2"));

      return GSON.toJson(metadataInfo);

    } catch (Exception e) {
      // If all else fails, return basic info
      java.util.Map<String, Object> metadataInfo = new java.util.HashMap<>();
      metadataInfo.put("location", table.location());
      metadataInfo.put("uuid", table.uuid());
      if (table.currentSnapshot() != null) {
        metadataInfo.put("current-snapshot-id", table.currentSnapshot().snapshotId());
      }
      return GSON.toJson(metadataInfo);
    }
  }

  /**
   * Extract metadata history (list of previous metadata.json files)
   *
   * @param table Iceberg table
   * @return List of metadata versions, or null if not available
   */
  private List<IcebergMetadata.MetadataVersion> extractMetadataHistory(Table table) {
    if (table == null) {
      return null;
    }

    try {
      List<IcebergMetadata.MetadataVersion> history = new java.util.ArrayList<>();

      // Try to get snapshot history entries
      List<org.apache.iceberg.HistoryEntry> historyEntries = table.history();

      if (historyEntries != null && !historyEntries.isEmpty()) {
        int version = 1;
        for (org.apache.iceberg.HistoryEntry entry : historyEntries) {
          history.add(
              IcebergMetadata.MetadataVersion.builder()
                  .version(version++)
                  .file("snapshot-" + entry.snapshotId())
                  .timestamp(entry.timestampMillis())
                  .location(table.location() + "/metadata/snapshot-" + entry.snapshotId())
                  .build());
        }
      }

      return history.isEmpty() ? null : history;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extract partition specs from metadata.json (no file scanning)
   *
   * @param table Iceberg table
   * @return JSON string representation of partition specs, or null if unpartitioned
   */
  private String extractPartitionsJson(Table table) {
    if (table == null || table.spec().isUnpartitioned()) {
      return null;
    }

    try {
      // Get table operations to access metadata
      org.apache.iceberg.TableOperations ops = ((org.apache.iceberg.BaseTable) table).operations();
      if (ops == null || ops.current() == null) {
        return null;
      }

      org.apache.iceberg.TableMetadata currentMetadata = ops.current();

      // Get partition specs from metadata
      java.util.List<org.apache.iceberg.PartitionSpec> partitionSpecs =
          new java.util.ArrayList<>(currentMetadata.specs());

      if (partitionSpecs.isEmpty()) {
        return null;
      }

      // Convert partition specs to JSON format
      java.util.List<java.util.Map<String, Object>> specsList = new java.util.ArrayList<>();
      for (org.apache.iceberg.PartitionSpec spec : partitionSpecs) {
        java.util.Map<String, Object> specInfo = new java.util.HashMap<>();
        specInfo.put("spec-id", spec.specId());

        // Extract fields
        java.util.List<java.util.Map<String, Object>> fieldsList = new java.util.ArrayList<>();
        for (org.apache.iceberg.PartitionField field : spec.fields()) {
          java.util.Map<String, Object> fieldInfo = new java.util.HashMap<>();
          fieldInfo.put("field-id", field.fieldId());
          fieldInfo.put("name", field.name());
          fieldInfo.put("transform", field.transform().toString());
          fieldInfo.put("source-id", field.sourceId());
          fieldsList.add(fieldInfo);
        }
        specInfo.put("fields", fieldsList);

        specsList.add(specInfo);
      }

      return GSON.toJson(specsList);

    } catch (Exception e) {
      // If reading partition specs fails, return null
      return null;
    }
  }

  /**
   * Gets entity (TableDto) representing a table if exists. Else throws NoSuchUserTableException.
   *
   * @param databaseId
   * @param tableId
   * @return TableDto
   */
  private TableDto getTableOrThrow(String databaseId, String tableId) {
    TableDtoPrimaryKey tableDtoPrimaryKey =
        TableDtoPrimaryKey.builder().databaseId(databaseId).tableId(tableId).build();

    Optional<TableDto> tableDto = openHouseInternalRepository.findById(tableDtoPrimaryKey);
    if (!tableDto.isPresent()) {
      throw new NoSuchUserTableException(databaseId, tableId);
    }
    return tableDto.get();
  }
}

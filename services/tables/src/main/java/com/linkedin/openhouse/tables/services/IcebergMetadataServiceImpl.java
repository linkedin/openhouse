package com.linkedin.openhouse.tables.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import com.google.gson.JsonSerializer;
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

  // Configure Gson to serialize Long values as strings to preserve precision in JavaScript
  private static final Gson GSON =
      new GsonBuilder()
          .registerTypeAdapter(
              Long.class,
              (JsonSerializer<Long>) (src, typeOfSrc, context) -> new JsonPrimitive(src.toString()))
          .registerTypeAdapter(
              long.class,
              (JsonSerializer<Long>) (src, typeOfSrc, context) -> new JsonPrimitive(src.toString()))
          .create();

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

    // Extract current snapshot ID ONLY from metadata.json to ensure precision and consistency
    // First try to extract from JSON string directly to avoid precision loss
    Long currentSnapshotId = extractCurrentSnapshotIdFromJsonString(currentMetadataJson);

    // Debug logging to track extraction
    if (currentSnapshotId != null) {
      System.out.println(
          "[IcebergMetadataService] Extracted currentSnapshotId from JSON string: "
              + currentSnapshotId);
    } else {
      // Fallback to parsing from JsonObject
      currentSnapshotId = extractCurrentSnapshotIdFromMetadata(metadataObj);
      if (currentSnapshotId != null) {
        System.out.println(
            "[IcebergMetadataService] Extracted currentSnapshotId from JsonObject: "
                + currentSnapshotId);
      } else {
        System.out.println(
            "[IcebergMetadataService] WARNING: Could not extract currentSnapshotId from"
                + " metadata.json");
      }
    }

    // DO NOT fallback to table.currentSnapshot() - only use metadata.json for consistency

    // Extract snapshots from metadata.json
    String snapshotsJson = extractSnapshotsFromMetadata(metadataObj);

    // Extract metadata history
    List<IcebergMetadata.MetadataVersion> metadataHistory = extractMetadataHistory(icebergTable);

    // Extract partition specs from metadata (no file scanning)
    String partitionsJson = extractPartitionsJson(icebergTable);

    // Build and return metadata model
    // Convert currentSnapshotId to String to preserve precision in JavaScript
    return IcebergMetadata.builder()
        .tableId(tableId)
        .databaseId(databaseId)
        .currentMetadata(currentMetadataJson)
        .metadataHistory(metadataHistory)
        .metadataLocation(icebergTable.location() + "/metadata")
        .snapshots(snapshotsJson)
        .partitions(partitionsJson)
        .currentSnapshotId(currentSnapshotId != null ? String.valueOf(currentSnapshotId) : null)
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
   * Extract current snapshot ID directly from metadata JSON string This bypasses Gson parsing to
   * avoid any precision loss with large numbers
   *
   * @param metadataJsonString Raw metadata JSON string
   * @return Current snapshot ID, or null if not available
   */
  private Long extractCurrentSnapshotIdFromJsonString(String metadataJsonString) {
    if (metadataJsonString == null || metadataJsonString.isEmpty()) {
      return null;
    }

    try {
      // Use regex to find "current-snapshot-id" : <number> in the JSON string
      // This preserves the exact number without any parsing precision loss
      java.util.regex.Pattern pattern =
          java.util.regex.Pattern.compile("\"current-snapshot-id\"\\s*:\\s*(\\d+)");
      java.util.regex.Matcher matcher = pattern.matcher(metadataJsonString);

      if (matcher.find()) {
        String snapshotIdStr = matcher.group(1);
        return Long.parseLong(snapshotIdStr);
      }
      return null;
    } catch (Exception e) {
      return null;
    }
  }

  /**
   * Extract current snapshot ID from parsed metadata.json Uses string parsing to avoid precision
   * loss with large numbers
   *
   * @param metadataObj Parsed metadata JSON object
   * @return Current snapshot ID, or null if not available
   */
  private Long extractCurrentSnapshotIdFromMetadata(JsonObject metadataObj) {
    if (metadataObj == null || !metadataObj.has("current-snapshot-id")) {
      return null;
    }

    try {
      // Get as JsonPrimitive to preserve precision
      com.google.gson.JsonPrimitive primitive =
          metadataObj.getAsJsonPrimitive("current-snapshot-id");
      if (primitive != null) {
        // Parse as string first to avoid double precision loss
        String snapshotIdStr = primitive.getAsString();
        return Long.parseLong(snapshotIdStr);
      }
      return null;
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

      // Try reading the metadata file directly as string to preserve precision
      for (String path : pathsToTry) {
        try {
          org.apache.iceberg.io.InputFile inputFile = io.newInputFile(path);

          // Read the file content directly as bytes, then convert to string
          // This preserves the exact JSON without any parsing/re-serialization
          byte[] bytes = new byte[(int) inputFile.getLength()];
          try (java.io.InputStream inputStream = inputFile.newStream()) {
            int bytesRead = inputStream.read(bytes);
            if (bytesRead > 0) {
              String jsonContent =
                  new String(bytes, 0, bytesRead, java.nio.charset.StandardCharsets.UTF_8);
              // Verify it's valid JSON by checking for basic structure
              if (jsonContent.contains("\"format-version\"")
                  && jsonContent.contains("\"location\"")) {
                return jsonContent;
              }
            }
          }

          // Fallback to parsing if direct read fails
          org.apache.iceberg.TableMetadata metadata =
              org.apache.iceberg.TableMetadataParser.read(io, inputFile);
          if (metadata != null) {
            // Convert metadata to JSON (may have precision loss for large numbers)
            return org.apache.iceberg.TableMetadataParser.toJson(metadata);
          }
        } catch (Exception e) {
          // Try next path
        }
      }

      // Fallback: return basic info if we can't parse the metadata file
      // IMPORTANT: Include snapshots array from table object to ensure frontend can display them
      // NOTE: Do NOT include current-snapshot-id here - only extract from metadata.json to avoid
      // precision loss
      java.util.Map<String, Object> metadataInfo = new java.util.HashMap<>();
      metadataInfo.put("location", table.location());
      metadataInfo.put("uuid", table.uuid());
      metadataInfo.put("format-version", table.properties().getOrDefault("format-version", "2"));

      // Add snapshots array from table object
      metadataInfo.put("snapshots", extractSnapshotsFromTable(table));

      return GSON.toJson(metadataInfo);

    } catch (Exception e) {
      // If all else fails, return basic info with snapshots
      // NOTE: Do NOT include current-snapshot-id here - only extract from metadata.json to avoid
      // precision loss
      java.util.Map<String, Object> metadataInfo = new java.util.HashMap<>();
      metadataInfo.put("location", table.location());
      metadataInfo.put("uuid", table.uuid());

      // Add snapshots array from table object
      metadataInfo.put("snapshots", extractSnapshotsFromTable(table));

      return GSON.toJson(metadataInfo);
    }
  }

  /**
   * Extract snapshots directly from Table object (fallback when metadata file can't be parsed)
   *
   * @param table Iceberg table
   * @return List of snapshot maps with id, timestamp, operation, summary, etc.
   */
  private List<java.util.Map<String, Object>> extractSnapshotsFromTable(Table table) {
    List<java.util.Map<String, Object>> snapshotsList = new ArrayList<>();

    if (table == null) {
      return snapshotsList;
    }

    try {
      // Get all snapshots from table.snapshots() iterator
      Iterable<org.apache.iceberg.Snapshot> snapshots = table.snapshots();
      if (snapshots != null) {
        for (org.apache.iceberg.Snapshot snapshot : snapshots) {
          java.util.Map<String, Object> snapshotInfo = new java.util.HashMap<>();

          snapshotInfo.put("snapshot-id", snapshot.snapshotId());
          snapshotInfo.put("timestamp-ms", snapshot.timestampMillis());

          // Get operation from snapshot summary
          if (snapshot.operation() != null) {
            snapshotInfo.put("operation", snapshot.operation());
          }

          // Get summary map
          if (snapshot.summary() != null && !snapshot.summary().isEmpty()) {
            snapshotInfo.put("summary", new java.util.HashMap<>(snapshot.summary()));
          }

          // Get parent snapshot id if exists
          if (snapshot.parentId() != null) {
            snapshotInfo.put("parent-snapshot-id", snapshot.parentId());
          }

          snapshotsList.add(snapshotInfo);
        }
      }
    } catch (Exception e) {
      // Return empty list on error
      return snapshotsList;
    }

    return snapshotsList;
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

  @Override
  public com.linkedin.openhouse.tables.model.IcebergMetadataDiff getMetadataDiff(
      String databaseId, String tableId, Long snapshotId, String actingPrincipal) {
    TableDto tableDto = getTableOrThrow(databaseId, tableId);
    authorizationUtils.checkTablePrivilege(
        tableDto, actingPrincipal, Privileges.GET_TABLE_METADATA);

    // Load the Iceberg table from catalog
    Table icebergTable = catalog.loadTable(TableIdentifier.of(databaseId, tableId));

    // Get table history to find snapshots in order
    List<org.apache.iceberg.HistoryEntry> historyEntries = icebergTable.history();
    if (historyEntries == null || historyEntries.isEmpty()) {
      throw new IllegalStateException("No snapshot history available for table");
    }

    // Find the current snapshot and previous snapshot in history
    org.apache.iceberg.HistoryEntry currentEntry = null;
    org.apache.iceberg.HistoryEntry previousEntry = null;
    boolean foundCurrent = false;

    for (int i = 0; i < historyEntries.size(); i++) {
      if (historyEntries.get(i).snapshotId() == snapshotId) {
        currentEntry = historyEntries.get(i);
        foundCurrent = true;
        // Get previous entry if it exists
        if (i > 0) {
          previousEntry = historyEntries.get(i - 1);
        }
        break;
      }
    }

    if (!foundCurrent) {
      throw new IllegalArgumentException(
          "Snapshot ID " + snapshotId + " not found in table history");
    }

    boolean isFirstCommit = (previousEntry == null);

    // Get the FileIO for accessing metadata files
    org.apache.iceberg.io.FileIO io = icebergTable.io();

    // Get current metadata.json to access metadata-log
    String currentMetadataJson = extractCurrentMetadata(icebergTable);
    JsonObject currentMetadataObj = GSON.fromJson(currentMetadataJson, JsonObject.class);

    // Extract metadata-log which contains the history of metadata files
    JsonArray metadataLog = null;
    if (currentMetadataObj != null && currentMetadataObj.has("metadata-log")) {
      metadataLog = currentMetadataObj.getAsJsonArray("metadata-log");
    }

    // Find metadata files for current and previous snapshots
    String currentMetadataFile = findMetadataFileForSnapshot(metadataLog, currentEntry);
    String previousMetadataFile =
        isFirstCommit ? null : findMetadataFileForSnapshot(metadataLog, previousEntry);

    // Fetch the actual metadata.json contents
    String currentMetadataContent =
        currentMetadataFile != null
            ? fetchMetadataFile(io, currentMetadataFile)
            : currentMetadataJson;
    String previousMetadataContent =
        previousMetadataFile != null ? fetchMetadataFile(io, previousMetadataFile) : null;

    // Build and return the diff
    // Convert snapshot IDs to strings to preserve precision in JavaScript
    return com.linkedin.openhouse.tables.model.IcebergMetadataDiff.builder()
        .tableId(tableId)
        .databaseId(databaseId)
        .currentMetadata(currentMetadataContent)
        .previousMetadata(previousMetadataContent)
        .currentSnapshotId(String.valueOf(currentEntry.snapshotId()))
        .previousSnapshotId(isFirstCommit ? null : String.valueOf(previousEntry.snapshotId()))
        .currentTimestamp(currentEntry.timestampMillis())
        .previousTimestamp(isFirstCommit ? null : previousEntry.timestampMillis())
        .currentMetadataLocation(currentMetadataFile)
        .previousMetadataLocation(previousMetadataFile)
        .isFirstCommit(isFirstCommit)
        .build();
  }

  /**
   * Find the metadata file location for a given snapshot from the metadata-log
   *
   * @param metadataLog The metadata-log array from metadata.json
   * @param historyEntry The history entry containing snapshot and timestamp
   * @return Metadata file location, or null if not found
   */
  private String findMetadataFileForSnapshot(
      JsonArray metadataLog, org.apache.iceberg.HistoryEntry historyEntry) {
    if (metadataLog == null || historyEntry == null) {
      return null;
    }

    // Find the metadata-log entry with matching or closest timestamp
    String closestFile = null;
    long minTimeDiff = Long.MAX_VALUE;

    for (JsonElement logElement : metadataLog) {
      JsonObject logEntry = logElement.getAsJsonObject();
      if (logEntry.has("timestamp-ms") && logEntry.has("metadata-file")) {
        long logTimestamp = logEntry.get("timestamp-ms").getAsLong();
        long timeDiff = Math.abs(logTimestamp - historyEntry.timestampMillis());

        if (timeDiff < minTimeDiff) {
          minTimeDiff = timeDiff;
          closestFile = logEntry.get("metadata-file").getAsString();
        }

        // If we find an exact match, use it
        if (timeDiff == 0) {
          break;
        }
      }
    }

    return closestFile;
  }

  /**
   * Fetch a metadata file from storage using FileIO
   *
   * @param io The FileIO instance for the table
   * @param metadataFilePath Path to the metadata file
   * @return JSON string of the metadata file content
   */
  private String fetchMetadataFile(org.apache.iceberg.io.FileIO io, String metadataFilePath) {
    try {
      org.apache.iceberg.io.InputFile inputFile = io.newInputFile(metadataFilePath);
      org.apache.iceberg.TableMetadata metadata =
          org.apache.iceberg.TableMetadataParser.read(io, inputFile);
      return org.apache.iceberg.TableMetadataParser.toJson(metadata);
    } catch (Exception e) {
      throw new RuntimeException("Failed to fetch metadata file: " + metadataFilePath, e);
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

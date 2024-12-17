package com.linkedin.openhouse.tables.utils;

import com.google.common.annotations.VisibleForTesting;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.StorageManager;
import com.linkedin.openhouse.common.exception.RequestValidationFailureException;
import com.linkedin.openhouse.internal.catalog.CatalogConstants;
import com.linkedin.openhouse.tables.api.spec.v0.request.CreateUpdateTableRequestBody;
import com.linkedin.openhouse.tables.api.spec.v0.request.IcebergSnapshotsRequestBody;
import com.linkedin.openhouse.tables.common.TableType;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * UUID Generator Class that: 1) Re-uses UUID from provided SnapshotJson's manifestList path 2)
 * Re-uses UUID from provided "openhouse.tableUUID" tableProperty 3) Generates a random UUID
 */
@Slf4j
@Component
public class TableUUIDGenerator {
  // TODO: r/w of tableProperties being managed in single place.
  private static final String OPENHOUSE_NAMESPACE = "openhouse.";
  private static final String DB_RAW_KEY = "databaseId";
  private static final String TBL_RAW_KEY = "tableId";
  private static final String TBL_LOC_RAW_KEY = "tableLocation";
  private static final String TBL_UUID_RAW_KEY = "tableUUID";

  @Autowired StorageManager storageManager;

  /**
   * Public api to generate UUID for a {@link CreateUpdateTableRequestBody}
   *
   * <p>1) use "openhouse.tableUUID" TableProperty based table path to generate UUID 2) generate
   * random UUID
   *
   * @param createUpdateTableRequestBody
   * @return UUID
   */
  public UUID generateUUID(CreateUpdateTableRequestBody createUpdateTableRequestBody) {
    return extractUUIDFromTableProperties(
            createUpdateTableRequestBody.getTableProperties(),
            createUpdateTableRequestBody.getDatabaseId(),
            createUpdateTableRequestBody.getTableId(),
            createUpdateTableRequestBody.getTableType())
        .orElseGet(UUID::randomUUID);
  }

  /**
   * Public api to generate UUID for a {@link IcebergSnapshotsRequestBody}
   *
   * <p>1) use Snapshot Json based table path to generate UUID 2) use "openhouse.tableUUID"
   * TableProperty based table path to generate UUID 3) generate random UUID
   *
   * @param icebergSnapshotsRequestBody
   * @return UUID
   */
  public UUID generateUUID(IcebergSnapshotsRequestBody icebergSnapshotsRequestBody) {
    return extractUUIDFromRequestBody(icebergSnapshotsRequestBody)
        .orElseGet(
            () -> generateUUID(icebergSnapshotsRequestBody.getCreateUpdateTableRequestBody()));
  }

  /** Simple helper method to obtain tableURI from requestBody. */
  private String getTableURI(IcebergSnapshotsRequestBody icebergSnapshotsRequestBody) {
    return icebergSnapshotsRequestBody.getCreateUpdateTableRequestBody().getDatabaseId()
        + "."
        + icebergSnapshotsRequestBody.getCreateUpdateTableRequestBody().getTableId();
  }

  /**
   * Extracting the value of given key from the table properties map. The main use cases are for
   * tableId, databaseId and tableLocation where the value captured in tblproperties preserved the
   * casing from creation. This casing is critical if r/w for this table occurs in a platform with
   * different casing-preservation contract.
   */
  private String extractFromTblPropsIfExists(
      String tableURI, Map<String, String> tblProps, String rawKey) {
    if (tblProps == null
        || !tblProps.containsKey(OPENHOUSE_NAMESPACE + rawKey)
        || tblProps.get(OPENHOUSE_NAMESPACE + rawKey) == null) {
      throw new RequestValidationFailureException(
          String.format(
              "Provided snapshot is invalid for %s since %s is missing in properties",
              tableURI, OPENHOUSE_NAMESPACE + rawKey));
    }
    return tblProps.get(OPENHOUSE_NAMESPACE + rawKey);
  }

  /**
   * Helper method to extract UUID from tableProperties. A CTAS command's commit() call provides
   * "openhouse.tableUUID", if snapshot was not provided, this property is used and its path is
   * validated. If tableType is REPLICA_TABLE, UUID is returned without path validation.
   *
   * <p>If tableProperties is null or doesn't contain "openhouse.tableUUID" returns empty optional.
   *
   * @param tableProperties
   * @param databaseId
   * @param tableId
   * @param tableType
   * @return Optional.of(UUID)
   */
  private Optional<UUID> extractUUIDFromTableProperties(
      Map<String, String> tableProperties, String databaseId, String tableId, TableType tableType) {
    Optional<String> tableUUIDProperty =
        Optional.ofNullable(tableProperties).map(x -> x.get(CatalogConstants.OPENHOUSE_UUID_KEY));

    if (!tableUUIDProperty.isPresent()) {
      return Optional.empty();
    }

    validatePathOfProvidedRequest(
        tableProperties, databaseId, tableId, tableUUIDProperty.get(), tableType);

    try {
      return tableUUIDProperty.map(UUID::fromString);
    } catch (IllegalArgumentException exception) {
      throw new RequestValidationFailureException(
          "TableProperty openhouse.TableUUID contains invalid UUID, internal exception:",
          exception);
    }
  }

  private void validatePathOfProvidedRequest(
      Map<String, String> tableProperties,
      String databaseId,
      String tableId,
      String tableUUIDProperty,
      TableType tableType) {

    String tableURI = String.format("%s.%s", databaseId, tableId);
    String dbIdFromProps = extractFromTblPropsIfExists(tableURI, tableProperties, DB_RAW_KEY);
    String tblIdFromProps = extractFromTblPropsIfExists(tableURI, tableProperties, TBL_RAW_KEY);

    if (TableType.REPLICA_TABLE != tableType) {
      // Extract tableLocation from table properties (openhouse.tableLocation)
      // tableLocation should be the absolute path to the latest metadata file including scheme.
      // Scheme is not present for HDFS and Local storages. See:
      // https://github.com/linkedin/openhouse/issues/121
      String tableLocation =
          extractFromTblPropsIfExists(tableURI, tableProperties, TBL_LOC_RAW_KEY);
      Storage storage = storageManager.getStorageFromPath(tableLocation);

      if (!storage.isPathValid(tableLocation, dbIdFromProps, tblIdFromProps, tableUUIDProperty)) {
        log.error("Previous tableLocation: {} doesn't exist", tableLocation);
        throw new RequestValidationFailureException(
            String.format("Provided snapshot is invalid for %s.%s", dbIdFromProps, tblIdFromProps));
      }
    }
  }

  /**
   * Helper method to extract UUID from Iceberg-Snapshots' RequestBody
   *
   * <p>If List is null or empty returns empty Optional. If List contains a snapshot, Snapshot is
   * validated by evaluating its "manifest-list" key.
   *
   * @param snapshotsRequestBody a complete snapshot request-body
   * @return Optional.of(UUID)
   */
  private Optional<UUID> extractUUIDFromRequestBody(
      IcebergSnapshotsRequestBody snapshotsRequestBody) {
    List<String> jsonSnapshots = snapshotsRequestBody.getJsonSnapshots();
    String tableURI =
        snapshotsRequestBody.getCreateUpdateTableRequestBody().getDatabaseId()
            + "."
            + snapshotsRequestBody.getCreateUpdateTableRequestBody().getTableId();
    String databaseId =
        extractFromTblPropsIfExists(
            tableURI,
            snapshotsRequestBody.getCreateUpdateTableRequestBody().getTableProperties(),
            DB_RAW_KEY);
    String tableId =
        extractFromTblPropsIfExists(
            tableURI,
            snapshotsRequestBody.getCreateUpdateTableRequestBody().getTableProperties(),
            TBL_RAW_KEY);

    String snapshotStr =
        Optional.ofNullable(jsonSnapshots)
            .filter(l -> !l.isEmpty())
            .map(l -> l.get(0))
            .orElse(null);

    if (snapshotStr == null) {
      return Optional.empty();
    }

    // Extract tableLocation from table properties (openhouse.tableLocation)
    // tableLocation should be the absolute path to the latest metadata file including scheme.
    // Scheme is not present for HDFS and Local storages. See:
    // https://github.com/linkedin/openhouse/issues/121
    String tableLocation =
        extractFromTblPropsIfExists(
            tableURI,
            snapshotsRequestBody.getCreateUpdateTableRequestBody().getTableProperties(),
            TBL_LOC_RAW_KEY);

    Storage storage = storageManager.getStorageFromPath(tableLocation);
    java.nio.file.Path databaseDirPath = Paths.get(storage.getClient().getRootPrefix(), databaseId);
    String manifestListKey = "manifest-list";
    java.nio.file.Path manifestListPath;
    try {
      String manifestListPathString =
          new Gson().fromJson(snapshotStr, JsonObject.class).get(manifestListKey).getAsString();
      manifestListPathString =
          StringUtils.removeStart(manifestListPathString, storage.getClient().getEndpoint());
      manifestListPath = Paths.get(manifestListPathString);
    } catch (Exception exception) {
      throw new RequestValidationFailureException(
          String.format(
              "Provided Snapshot %s doesn't contain metadata for %s", snapshotStr, manifestListKey),
          exception);
    }
    return Optional.of(
        extractUUIDFromExistingManifestListPath(manifestListPath, databaseDirPath, tableId));
  }

  /**
   * Given databaseDirPath ("/tmp/db") and manifestListPath ("/tmp/db/ctas-<UUID>/metadata/...avro")
   * and tableName. Validates the following and returns the UUID: 1) databaseDirPath is part of
   * manifestListPath 2) manifestListPath contains tableId, ex: "ctas-UUID" contains tableId "ctas"
   *
   * @return UUID
   */
  @VisibleForTesting
  private UUID extractUUIDFromExistingManifestListPath(
      java.nio.file.Path manifestListPath, java.nio.file.Path databaseDirPath, String tableId) {
    boolean isProperPathPrefix =
        manifestListPath.startsWith(databaseDirPath)
            && databaseDirPath.getNameCount() < manifestListPath.getNameCount();
    if (!isProperPathPrefix) {
      log.error(
          "Provided Snapshot location is incorrect, should be in: {}, but provided {}",
          databaseDirPath,
          manifestListPath);
      throw new RequestValidationFailureException(
          String.format("Provided snapshot is invalid for %s", tableId));
    }
    java.nio.file.Path tableDirectoryPath =
        manifestListPath
            .subpath(databaseDirPath.getNameCount(), manifestListPath.getNameCount())
            .iterator()
            .next();
    /* check if tableDirectory is of the form "tablename-<UUID>", if it is, extract UUID */
    String tableIdDash = String.format("%s-", tableId);
    if (!tableDirectoryPath.toString().startsWith(tableIdDash)) {
      log.error(
          "Provided Snapshot location is incorrect, should have table name: {}_, but provided {}",
          tableId,
          tableDirectoryPath);
      throw new RequestValidationFailureException(
          String.format("Provided snapshot is invalid for %s", tableId));
    }
    try {
      return UUID.fromString(tableDirectoryPath.toString().replaceFirst(tableIdDash, ""));
    } catch (IllegalArgumentException exception) {
      log.error("Table location {} contains invalid UUID", manifestListPath);
      throw new RequestValidationFailureException(
          "Provided snapshot is invalid, contains invalid UUID", exception);
    }
  }
}

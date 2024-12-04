package com.linkedin.openhouse.cluster.storage;

import com.google.common.base.Preconditions;
import java.util.Map;

/**
 * The Storage interface represents a storage system in OpenHouse. It provides methods to check if
 * the storage is configured, retrieve properties of the storage, get the type of the storage, and
 * get a client to interact with the storage.
 *
 * <p>Implementations of this interface should provide the specific logic for each type of storage.
 * For example, the {@link com.linkedin.openhouse.cluster.storage.local.LocalStorage} class is an
 * implementation of this interface for local storage, and it uses a {@link
 * com.linkedin.openhouse.cluster.storage.local.LocalStorageClient} to interact with the local file
 * system.
 */
public interface Storage {

  /**
   * Check if the storage is configured.
   *
   * <p>The storage is considered configured if {@link
   * com.linkedin.openhouse.cluster.storage.configs.StorageProperties} has type defined for it
   *
   * @return true if the storage is configured, false otherwise
   */
  boolean isConfigured();

  /**
   * Get the properties of the storage.
   *
   * @return a map of properties of the storage
   */
  Map<String, String> getProperties();

  /**
   * Get the type of the storage.
   *
   * <p>Please refer to {@link StorageType} for the list of supported storage types. An example type
   * of the local storage that can be returned {@link StorageType.Type#LOCAL}.
   *
   * @return the type of the storage
   */
  StorageType.Type getType();

  /**
   * Get a client to interact with the storage.
   *
   * @return a client to interact with the storage
   */
  StorageClient<?> getClient();

  /**
   * Allocates Table Storage and return location.
   *
   * <p>Allocating involves creating directory structure/ creating bucket, and setting appropriate
   * permissions etc. Please note that this method should avoid creating TableFormat specific
   * directories (ex: /data, /metadata for Iceberg or _delta_log for DeltaLake). Such provisioning
   * should be done by the TableFormat implementation.
   *
   * <p>After allocation is done, this method should return the table location where the table data
   * should be stored. Example: hdfs:///rootPrefix/databaseId/tableId-UUID for HDFS storage;
   * file:/tmp/databaseId/tableId-UUID for Local storage; s3://bucket/databaseId/tableId-UUID for S3
   * storage
   *
   * @param databaseId the database id of the table
   * @param tableId the table id of the table
   * @param tableUUID the UUID of the table
   * @param tableCreator the creator of the table
   * @return the table location after provisioning is done
   */
  String allocateTableLocation(
      String databaseId, String tableId, String tableUUID, String tableCreator);

  /**
   * Checks if the provided path is valid for this table. It returns true if the provided path
   * starts with the table location. Example: Hdfs TableLocation: /root/path/db/table-uuid path:
   * /root/path/db/table-uuid/metadata/metadata.json - returns true if file exists s3 TableLocation:
   * s3://bucket/root/path/db/table-uuid path: s3://bucket/root/path/db/table-uuid/data/file.orc -
   * returns true if file exists
   *
   * @param path path to a file/object
   * @return true if it's under the table location directory
   */
  default boolean isPathValid(String path, String databaseId, String tableId, String tableUUID) {
    String tableLocationPrefix = allocateTableLocation(databaseId, tableId, tableUUID, "");
    Preconditions.checkArgument(
        path.startsWith(tableLocationPrefix),
        String.format("%s is not under table location %s", path, tableLocationPrefix));
    return getClient().exists(path);
  }
}

package com.linkedin.openhouse.cluster.storage;

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
   * should be stored. Example: /rootPrefix/databaseId/tableId-UUID for HDFS storage;
   * /tmp/databaseId/tableId-UUID for Local storage; s3://bucket/databaseId/tableId-UUID for S3
   * storage
   *
   * @param databaseId the database id of the table
   * @param tableId the table id of the table
   * @param tableUUID the UUID of the table
   * @param tableCreator the creator of the table
   * @param skipProvisioning Set to true if heavy-lifting allocation work needs to be skipped and
   *     only the table location needs to be returned
   * @return the table location after provisioning is done
   */
  String allocateTableLocation(
      String databaseId,
      String tableId,
      String tableUUID,
      String tableCreator,
      boolean skipProvisioning);
}

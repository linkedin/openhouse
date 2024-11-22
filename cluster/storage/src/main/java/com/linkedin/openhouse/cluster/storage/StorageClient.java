package com.linkedin.openhouse.cluster.storage;

/**
 * The StorageClient interface represents a client to interact with a storage system in OpenHouse.
 * It provides a method to get the native client of the storage system.
 *
 * <p>Implementations of this interface should provide the specific logic for each type of storage
 * client. For example, the {@link com.linkedin.openhouse.cluster.storage.local.LocalStorageClient}
 * class is an implementation of this interface for local storage, and it uses an Apache Hadoop
 * {@link org.apache.hadoop.fs.FileSystem} to interact with the local file system.
 *
 * @param <T> the type of the native client of the storage system
 */
public interface StorageClient<T> {

  /**
   * Get the native client of the storage system.
   *
   * @return the native client of the storage system
   */
  T getNativeClient();

  /**
   * Get the storage type for the storage system.
   *
   * @return the storage type. Examples include "HDFS", "S3" etc.
   */
  StorageType.Type getStorageType();

  /**
   * Get the endpoint of the storage system.
   *
   * <p>Example: For HDFS, the endpoint could be "hdfs://localhost:9000". For local file system, the
   * endpoint could be "file://". For S3, the endpoint could be "s3://".
   *
   * @return the endpoint of the storage system
   */
  String getEndpoint();

  /**
   * Get the root prefix for OpenHouse on the storage system.
   *
   * <p>Root prefix should include the bucket-name plus any additional path components. Example: For
   * HDFS, the root prefix could be "/data/openhouse". For local file system, the root prefix could
   * be "/tmp". For S3, the root prefix could be "/bucket-name/key/prefix/to/openhouse".
   *
   * @return the root prefix for OpenHouse on the storage system
   */
  String getRootPrefix();

  /**
   * Checks if the path exists on the backend storage. Path is the absolute path to file including
   * scheme. Scheme is not prefix for local and hdfs storage. See:
   * https://github.com/linkedin/openhouse/issues/121
   *
   * @param path absolute path to a file including scheme
   * @return true if path exists else false
   */
  boolean fileExists(String path);
}

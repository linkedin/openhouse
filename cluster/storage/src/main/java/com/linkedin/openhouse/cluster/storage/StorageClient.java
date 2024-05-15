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
   * HDFS, the root path could be "/data/openhouse". For local file system, the root path could be
   * "/tmp". For S3, the root path could be "/bucket-name/key/prefix/to/openhouse".
   *
   * @return the root prefix for OpenHouse on the storage system
   */
  String getRootPrefix();
}

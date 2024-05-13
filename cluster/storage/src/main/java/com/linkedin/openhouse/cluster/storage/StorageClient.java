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

  String getEndpoint();

  String getRootPath();
}

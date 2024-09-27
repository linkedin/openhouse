package com.linkedin.openhouse.cluster.storage;

import com.linkedin.openhouse.cluster.configs.ClusterProperties;

/**
 * Interface between OpenHouse to underlying storage layer. Objects like {@link ClusterProperties}
 * and table format specific objects should all be abstracted away by this interface.
 *
 * @param <T> Types of storage client to provide.
 *     <p>WARNING: This interface is in alpha version and could be subject to change.
 */
public interface StorageProvider<T> {

  /** @return recognizable name of this storage object. */
  String name();

  /** @return The storage cluster's base URI. */
  String baseUri();

  /**
   * @return The storage type of the underlying medium for OpenHouse. Typical value includes Hadoop,
   *     S3, GCP, etc.
   */
  String storageType();

  /**
   * @return The storage client handle. Typical example includes {@link *
   *     org.apache.hadoop.fs.FileSystem} and GCP's Storage class.
   */
  T storageClient();
}

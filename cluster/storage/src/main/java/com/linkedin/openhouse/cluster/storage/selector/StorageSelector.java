package com.linkedin.openhouse.cluster.storage.selector;

import com.linkedin.openhouse.cluster.storage.Storage;
import com.linkedin.openhouse.cluster.storage.selector.impl.DefaultStorageSelector;

/**
 * The StorageSelector interface provides a way to select storage per table
 *
 * <p>Implementations of this interface can choose to return different storages for different db and
 * tables For Example {@link DefaultStorageSelector} returns the cluster level storage for all
 * tables
 */
public interface StorageSelector {
  /**
   * Select the storage for given db and table. This call should be idempotent, same db and table
   * should return same storage
   *
   * @param db
   * @param table
   * @return {@link Storage}
   */
  Storage selectStorage(String db, String table);

  /**
   * Get the Simple class name of the implementing class. This name will be used to configure the
   * Storage Selector from cluster.yaml
   *
   * @return Simple name of implementing class
   */
  String getName();
}

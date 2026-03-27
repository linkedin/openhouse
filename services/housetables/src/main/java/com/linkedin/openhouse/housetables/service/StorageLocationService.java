package com.linkedin.openhouse.housetables.service;

import com.linkedin.openhouse.housetables.api.spec.model.StorageLocation;
import java.util.List;

/** Service for managing {@link StorageLocation} entities and their association to tables. */
public interface StorageLocationService {

  /** Create a new StorageLocation with a generated ID. Returns the persisted entity. */
  StorageLocation createStorageLocation(String uri);

  /**
   * Get a StorageLocation by its ID. Throws {@link
   * com.linkedin.openhouse.common.exception.NoSuchEntityException} if not found.
   */
  StorageLocation getStorageLocation(String storageLocationId);

  /** Get all StorageLocations associated with a table. */
  List<StorageLocation> getStorageLocationsForTable(String databaseId, String tableId);

  /** Create a new StorageLocation with a generated ID and no URI. Returns the persisted entity. */
  StorageLocation createStorageLocation();

  /** Update the URI of an existing StorageLocation. Returns the updated entity. */
  StorageLocation updateStorageLocationUri(String storageLocationId, String uri);

  /** Associate an existing StorageLocation with a table. */
  void addStorageLocationToTable(String databaseId, String tableId, String storageLocationId);
}

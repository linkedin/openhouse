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

  /** Get all StorageLocations associated with a table by its UUID. */
  List<StorageLocation> getStorageLocationsForTable(String tableUuid);

  /** Create a new StorageLocation with a generated ID and no URI. Returns the persisted entity. */
  StorageLocation createStorageLocation();

  /** Update the URI of an existing StorageLocation. Returns the updated entity. */
  StorageLocation updateStorageLocationUri(String storageLocationId, String uri);

  /** Associate an existing StorageLocation with a table by its UUID. */
  void addStorageLocationToTable(String tableUuid, String storageLocationId);
}

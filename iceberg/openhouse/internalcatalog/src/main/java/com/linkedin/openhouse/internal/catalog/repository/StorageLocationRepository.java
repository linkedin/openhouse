package com.linkedin.openhouse.internal.catalog.repository;

import com.linkedin.openhouse.internal.catalog.model.StorageLocationDto;
import java.util.List;

/**
 * Repository interface for managing StorageLocations for tables. Implementations call the
 * HouseTables Service via HTTP.
 */
public interface StorageLocationRepository {

  /** Create a new StorageLocation at the given URI. Returns the persisted entity with its ID. */
  StorageLocationDto createStorageLocation(String uri);

  /** Create a new StorageLocation with no URI. Returns the persisted entity with its ID. */
  StorageLocationDto allocateStorageLocation();

  /** Update the URI of an existing StorageLocation. */
  StorageLocationDto updateStorageLocationUri(String storageLocationId, String uri);

  /** Get a StorageLocation by its ID. */
  StorageLocationDto getStorageLocation(String storageLocationId);

  /** Get all StorageLocations associated with a table. */
  List<StorageLocationDto> getStorageLocationsForTable(String databaseId, String tableId);

  /** Associate an existing StorageLocation with a table. */
  void addStorageLocationToTable(String databaseId, String tableId, String storageLocationId);
}

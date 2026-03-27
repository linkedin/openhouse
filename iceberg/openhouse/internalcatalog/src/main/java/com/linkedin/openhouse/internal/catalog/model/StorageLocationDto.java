package com.linkedin.openhouse.internal.catalog.model;

import lombok.Builder;
import lombok.Value;

/**
 * DTO representing a StorageLocation returned from the HouseTables Service. A StorageLocation is a
 * URI at which an Iceberg table's files reside. One per table is considered active (the one
 * matching the current metadata.location base path); historical locations remain associated to the
 * table until drained.
 */
@Builder
@Value
public class StorageLocationDto {
  /** Unique identifier for this storage location. */
  String storageLocationId;

  /** Base URI for all Iceberg files at this location. Opaque to OpenHouse. */
  String uri;
}

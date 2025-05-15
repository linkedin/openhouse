package com.linkedin.openhouse.housetables.dto.model;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import lombok.Builder;
import lombok.Value;

/**
 * Needed to avoid coupling the transport and controller layer by sharing the same {@link UserTable}
 * object.
 */
@Builder(toBuilder = true)
@Value
public class UserTableDto {
  String tableId;

  String databaseId;

  String tableVersion;

  String metadataLocation;

  String storageType;

  Long creationTime;
}

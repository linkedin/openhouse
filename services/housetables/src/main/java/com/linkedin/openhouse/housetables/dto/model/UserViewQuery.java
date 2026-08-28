package com.linkedin.openhouse.housetables.dto.model;

import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import lombok.Builder;
import lombok.Value;

/**
 * The filter the view query path accepts, carrying only the fields that path reads. Needed to avoid
 * coupling the transport and service layer by sharing the same {@link UserTable} object.
 */
@Builder(toBuilder = true)
@Value
public class UserViewQuery {
  String tableId;

  String databaseId;

  String tableVersion;

  String metadataLocation;

  String storageType;

  Long creationTime;
}

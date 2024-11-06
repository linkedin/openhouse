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

  /**
   * Compare if a given {@link UserTable} matches the current one if all non-null fields are equal.
   *
   * @param userTable the object to be examined.
   * @return true if matched.
   */
  public boolean match(UserTableDto userTable) {
    return Utilities.fieldMatchCaseInsensitive(this.databaseId, userTable.databaseId)
        && Utilities.fieldMatchCaseInsensitive(this.tableId, userTable.tableId)
        && Utilities.fieldMatch(this.metadataLocation, userTable.metadataLocation)
        && Utilities.fieldMatch(this.tableVersion, userTable.tableVersion)
        && Utilities.fieldMatch(this.storageType, userTable.storageType)
        && Utilities.fieldMatch(this.creationTime, userTable.creationTime);
  }
}

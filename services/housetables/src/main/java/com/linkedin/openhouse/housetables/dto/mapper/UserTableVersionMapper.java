package com.linkedin.openhouse.housetables.dto.mapper;

import com.linkedin.openhouse.common.api.validator.ValidatorConstants;
import com.linkedin.openhouse.common.exception.EntityConcurrentModificationException;
import com.linkedin.openhouse.housetables.api.spec.model.UserTable;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import java.util.Optional;
import org.mapstruct.Context;
import org.mapstruct.Mapper;
import org.mapstruct.Named;

/**
 * A {@link UserTablesMapper}'s plugin that correctly extracts a version from a {@link UserTable}
 * and an existing {@link UserTableRow}. This plugin helps in mapping a version number to the
 * corresponding metadata location.
 */
@Mapper(componentModel = "spring")
public class UserTableVersionMapper {
  @Named("toVersion")
  public Long toVersion(UserTable userTable, @Context Optional<UserTableRow> existingUserTableRow) {
    if (!existingUserTableRow.isPresent()) {
      if (!userTable.getTableVersion().equals(ValidatorConstants.INITIAL_TABLE_VERSION)) {
        throw new EntityConcurrentModificationException(
            String.format(
                "databaseId : %s, tableId : %s %s",
                userTable.getDatabaseId(),
                userTable.getTableId(),
                "The requested user table has been deleted by other processes."),
            new RuntimeException());
      }
      return null;
    } else {
      if (existingUserTableRow.get().getMetadataLocation().equals(userTable.getTableVersion())) {
        return existingUserTableRow.get().getVersion();
      } else {
        throw new EntityConcurrentModificationException(
            String.format(
                "databaseId : %s, tableId : %s, version: %s %s",
                userTable.getDatabaseId(),
                userTable.getTableId(),
                userTable.getTableVersion(),
                "The requested user table has been modified/created by other processes."),
            new RuntimeException());
      }
    }
  }
}

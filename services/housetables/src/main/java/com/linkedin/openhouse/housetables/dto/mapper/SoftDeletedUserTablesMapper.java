package com.linkedin.openhouse.housetables.dto.mapper;

import com.linkedin.openhouse.housetables.dto.model.UserTableDto;
import com.linkedin.openhouse.housetables.model.SoftDeletedUserTableRow;
import com.linkedin.openhouse.housetables.model.UserTableRow;
import java.time.Instant;
import org.mapstruct.AfterMapping;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.MappingTarget;

@Mapper(componentModel = "spring")
public interface SoftDeletedUserTablesMapper {

  // TODO: This should be a configurable value based off of database-level configurations
  // Default to 7 days in seconds
  static final int DEFAULT_PURGE_AFTER_SECONDS = 7 * 24 * 60 * 60;

  /**
   * Dropping the discriminator is intentional and lossless: views are hard deleted, so this store
   * can only ever hold tables.
   */
  SoftDeletedUserTableRow toSoftDeletedUserTableRow(UserTableRow userTableRow);

  @Mapping(target = "tableVersion", source = "metadataLocation")
  UserTableDto toUserTableDto(SoftDeletedUserTableRow softDeletedUserTableRow);

  @AfterMapping
  default void calculateDeleteTimeAndTimeToLive(
      @MappingTarget
          SoftDeletedUserTableRow.SoftDeletedUserTableRowBuilder softDeletedUserTableRowBuilder) {
    Instant deleteTime = Instant.now();
    softDeletedUserTableRowBuilder.deletedAtMs(deleteTime.toEpochMilli());

    softDeletedUserTableRowBuilder.purgeAfterMs(
        deleteTime.plusSeconds(DEFAULT_PURGE_AFTER_SECONDS).toEpochMilli());
  }
}
